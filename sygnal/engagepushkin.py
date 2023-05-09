import base64
import json
import logging
import time
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Any, AnyStr, Dict, List, Optional, Tuple

from opentracing import Span, tags
from prometheus_client import Counter, Gauge, Histogram
from twisted.internet.defer import DeferredSemaphore
from twisted.web.client import FileBodyProducer, HTTPConnectionPool, readBody
from twisted.web.http_headers import Headers
from twisted.web.iweb import IResponse

from sygnal.exceptions import (
    NotificationDispatchException,
    PushkinSetupException,
    TemporaryNotificationDispatchException,
)
from sygnal.helper.context_factory import ClientTLSOptionsFactory
from sygnal.helper.proxy.proxyagent_twisted import ProxyAgent
from sygnal.notifications import (
    ConcurrencyLimitedPushkin,
    Device,
    Notification,
    NotificationContext,
)
from sygnal.utils import NotificationLoggerAdapter, json_decoder, twisted_sleep

if TYPE_CHECKING:
    from sygnal.sygnal import Sygnal

QUEUE_TIME_HISTOGRAM = Histogram(
    "sygnal_gcm_queue_time", "Time taken waiting for a connection to ENGAGE"
)

SEND_TIME_HISTOGRAM = Histogram(
    "sygnal_gcm_request_time", "Time taken to send HTTP request to ENGAGE"
)

PENDING_REQUESTS_GAUGE = Gauge(
    "sygnal_pending_gcm_requests", "Number of ENGAGE requests waiting for a connection"
)

ACTIVE_REQUESTS_GAUGE = Gauge(
    "sygnal_active_gcm_requests", "Number of ENGAGE requests in flight"
)

RESPONSE_STATUS_CODES_COUNTER = Counter(
    "sygnal_gcm_status_codes",
    "Number of HTTP response status codes received from ENGAGE",
    labelnames=["pushkin", "code"],
)

logger = logging.getLogger(__name__)

ENGAGE_URL = b"https://push.api.engagelab.cc/v3/push"
MAX_TRIES = 3
RETRY_DELAY_BASE = 10
MAX_BYTES_PER_FIELD = 1024

# The error codes that mean a registration ID will never
# succeed and we should reject it upstream.
# We include NotRegistered here too for good measure, even
# though engage-client 'helpfully' extracts these into a separate
# list.
BAD_PUSHKEY_FAILURE_CODES = [
    "MissingRegistration",
    "InvalidRegistration",
    "NotRegistered",
    "InvalidPackageName",
    "MismatchSenderId",
]

# Failure codes that mean the message in question will never
# succeed, so don't retry, but the registration ID is fine
# so we should not reject it upstream.
BAD_MESSAGE_FAILURE_CODES = ["MessageTooBig", "InvalidDataKey", "InvalidTtl"]

DEFAULT_MAX_CONNECTIONS = 20


class EngagePushkin(ConcurrencyLimitedPushkin):
    """
    Pushkin that relays notifications to Engage Messaging.
    """

    UNDERSTOOD_CONFIG_FIELDS = {
                                   "type",
                                   "api_key",
                                   "fcm_options",
                                   "max_connections",
                               } | ConcurrencyLimitedPushkin.UNDERSTOOD_CONFIG_FIELDS

    def __init__(self, name: str, sygnal: "Sygnal", config: Dict[str, Any]) -> None:
        super().__init__(name, sygnal, config)

        nonunderstood = set(self.cfg.keys()).difference(self.UNDERSTOOD_CONFIG_FIELDS)
        if len(nonunderstood) > 0:
            logger.warning(
                "The following configuration fields are not understood: %s",
                nonunderstood,
            )

        self.http_pool = HTTPConnectionPool(reactor=sygnal.reactor)
        self.max_connections = self.get_config(
            "max_connections", int, DEFAULT_MAX_CONNECTIONS
        )
        if "intent_app_key" in config:
            self.app_key = config["intent_app_key"]
        else:
            self.app_key = "com.aplink.wallet.dev"
        logger.info("Engagepushkin's app_key: %s", self.app_key)
        self.connection_semaphore = DeferredSemaphore(self.max_connections)
        self.http_pool.maxPersistentPerHost = self.max_connections

        tls_client_options_factory = ClientTLSOptionsFactory()

        # use the Sygnal global proxy configuration
        proxy_url = sygnal.config.get("proxy")

        self.http_agent = ProxyAgent(
            reactor=sygnal.reactor,
            pool=self.http_pool,
            contextFactory=tls_client_options_factory,
            proxy_url_str=proxy_url,
        )

        self.api_key = self.get_config("api_key", str)
        if not self.api_key:
            raise PushkinSetupException("No API key set in config")

        # Use the fcm_options config dictionary as a foundation for the body;
        # this lets the Sygnal admin choose custom FCM options
        # (e.g. content_available).
        self.base_request_body = self.get_config("fcm_options", dict, {})
        if not isinstance(self.base_request_body, dict):
            raise PushkinSetupException(
                "Config field fcm_options, if set, must be a dictionary of options"
            )

    @classmethod
    async def create(
            cls, name: str, sygnal: "Sygnal", config: Dict[str, Any]
    ) -> "EngagePushkin":
        """
        Override this if your pushkin needs to call async code in order to
        be constructed. Otherwise, it defaults to just invoking the Python-standard
        __init__ constructor.

        Returns:
            an instance of this Pushkin
        """
        return cls(name, sygnal, config)

    async def _perform_http_request(
            self, body: Dict, headers: Dict[AnyStr, List[AnyStr]]
    ) -> Tuple[IResponse, str]:
        """
        Perform an HTTP request to the FCM server with the body and headers
        specified.
        Args:
            body: Body. Will be JSON-encoded.
            headers: HTTP Headers.

        Returns:

        """
        body_producer = FileBodyProducer(BytesIO(json.dumps(body).encode()))

        # we use the semaphore to actually limit the number of concurrent
        # requests, since the HTTPConnectionPool will actually just lead to more
        # requests being created but not pooled – it does not perform limiting.
        with QUEUE_TIME_HISTOGRAM.time():
            with PENDING_REQUESTS_GAUGE.track_inprogress():
                await self.connection_semaphore.acquire()

        try:
            with SEND_TIME_HISTOGRAM.time():
                with ACTIVE_REQUESTS_GAUGE.track_inprogress():
                    print("Engage notify url: %s, headers: %s, body: %s", ENGAGE_URL, headers, body)
                    response = await self.http_agent.request(
                        b"POST",
                        ENGAGE_URL,
                        headers=Headers(headers),
                        bodyProducer=body_producer,
                    )
                    response_text = (await readBody(response)).decode()
        except Exception as exception:
            raise TemporaryNotificationDispatchException(
                "Engage request failure"
            ) from exception
        finally:
            self.connection_semaphore.release()
        return response, response_text

    async def _request_dispatch(
            self,
            n: Notification,
            log: NotificationLoggerAdapter,
            body: dict,
            headers: Dict[AnyStr, List[AnyStr]],
            pushkeys: List[str],
            span: Span,
    ) -> Tuple[List[str], List[str]]:
        poke_start_time = time.time()

        failed = []

        response, response_text = await self._perform_http_request(body, headers)
        print('response_text: %s' % response_text)

        RESPONSE_STATUS_CODES_COUNTER.labels(
            pushkin=self.name, code=response.code
        ).inc()

        log.debug("Engage request took %f seconds", time.time() - poke_start_time)

        span.set_tag(tags.HTTP_STATUS_CODE, response.code)

        if 500 <= response.code < 600:
            log.debug("%d from server, waiting to try again", response.code)

            retry_after = None

            for header_value in response.headers.getRawHeaders(
                    b"retry-after", default=[]
            ):
                retry_after = int(header_value)
                span.log_kv({"event": "engage_retry_after", "retry_after": retry_after})

            raise TemporaryNotificationDispatchException(
                "ENGAGE server error, hopefully temporary.", custom_retry_delay=retry_after
            )
        elif response.code == 400:
            log.error(
                "%d from server, we have sent something invalid! Error: %r",
                response.code,
                response_text,
            )
            # permanent failure: give up
            raise NotificationDispatchException("Invalid request")
        elif response.code == 401:
            log.error(
                "401 from server! Our API key is invalid? Error: %r", response_text
            )
            # permanent failure: give up
            raise NotificationDispatchException("Not authorised to push")
        elif response.code == 404:
            # assume they're all failed
            log.info("Reg IDs %r get 404 response; assuming unregistered", pushkeys)
            return pushkeys, []
        elif 200 <= response.code < 300:
            try:
                resp_object = json_decoder.decode(response_text)
            except ValueError:
                raise NotificationDispatchException("Invalid JSON response from ENGAGE.")
            ##
            if "msg_id" not in resp_object:
                log.error(
                    "%d from server but response contained no 'msg_id' key: %r",
                    response.code,
                    response_text,
                )
            resp_object["results"] = ["OK"]
            print("send ok...")
            ## 之前的GCM逻辑 ##
            # if "results" not in resp_object:
            #     log.error(
            #         "%d from server but response contained no 'results' key: %r",
            #         response.code,
            #         response_text,
            #     )
            # if len(resp_object["results"]) < len(pushkeys):
            #     log.error(
            #         "Sent %d notifications but only got %d responses!",
            #         len(n.devices),
            #         len(resp_object["results"]),
            #     )
            #     span.log_kv(
            #         {
            #             logs.EVENT: "engage_response_mismatch",
            #             "num_devices": len(n.devices),
            #             "num_results": len(resp_object["results"]),
            #         }
            #     )
            ## 之前的GCM逻辑 ##

            # determine which pushkeys to retry or forget about
            new_pushkeys = []
            for i, result in enumerate(resp_object["results"]):
                if "error" in result:
                    log.warning(
                        "Error for pushkey %s: %s", pushkeys[i], result["error"]
                    )
                    span.set_tag("engage_error", result["error"])
                    if result["error"] in BAD_PUSHKEY_FAILURE_CODES:
                        log.info(
                            "Reg ID %r has permanently failed with code %r: "
                            "rejecting upstream",
                            pushkeys[i],
                            result["error"],
                        )
                        failed.append(pushkeys[i])
                    elif result["error"] in BAD_MESSAGE_FAILURE_CODES:
                        log.info(
                            "Message for reg ID %r has permanently failed with code %r",
                            pushkeys[i],
                            result["error"],
                        )
                    else:
                        log.info(
                            "Reg ID %r has temporarily failed with code %r",
                            pushkeys[i],
                            result["error"],
                        )
                        new_pushkeys.append(pushkeys[i])
            return failed, new_pushkeys
        else:
            raise NotificationDispatchException(
                f"Unknown ENGAGE response code {response.code}"
            )

    async def _dispatch_notification_unlimited(
            self, n: Notification, device: Device, context: NotificationContext
    ) -> List[str]:
        log = NotificationLoggerAdapter(logger, {"request_id": context.request_id})

        # `_dispatch_notification_unlimited` gets called once for each device in the
        # `Notification` with a matching app ID. We do something a little dirty and
        # perform all of our dispatches the first time we get called for a
        # `Notification` and do nothing for the rest of the times we get called.
        print("Notification : type: %s, %s, room_id: %s, unread: %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, device.pushkey: %s, %s, %s" %
              (n.type, n.user_is_target, n.room_id, n.counts.unread, n.counts.missed_calls, n.prio, n.event_id, n.content, n.membership, n.room_id, n.sender, n.sender_display_name, device.app_id, device.pushkey, device.data
               , device.tweaks.sound))
        pushkeys = [
            device.pushkey for device in n.devices if self.handles_appid(device.app_id)
        ]
        # `pushkeys` ought to never be empty here. At the very least it should contain
        # `device`'s pushkey.

        if pushkeys[0] != device.pushkey:
            # We've already been asked to dispatch for this `Notification` and have
            # previously sent out the notification to all devices.
            return []

        # The pushkey is kind of secret because you can use it to send push
        # to someone.
        # span_tags = {"pushkeys": pushkeys}
        span_tags = {"engage_num_devices": len(pushkeys)}

        with self.sygnal.tracer.start_span(
                "engage_dispatch", tags=span_tags, child_of=context.opentracing_span
        ) as span_parent:
            # TODO: Implement collapse_key to queue only one message per room.
            failed: List[str] = []

            data = EngagePushkin._build_data(n, device)

            # Reject pushkey(s) if default_payload is misconfigured
            if data is None:
                log.warning(
                    "Rejecting pushkey(s) due to misconfigured default_payload, "
                    "please ensure that default_payload is a dict."
                )
                return pushkeys

            headers = {
                "User-Agent": ["sygnal"],
                "Content-Type": ["application/json"],
                # 设置极光API的认证
                "Authorization": [self.get_jiguang_authorization_header(self.api_key)],
            }

            body = self.base_request_body.copy()
            body["data"] = data
            body["priority"] = "normal" if n.prio == "low" else "high"
            # Body['data'] sample:  {'event_id': '111', 'type': None, 'sender': None, 'room_name': None, 'room_alias': None,
            # 'membership': None, 'sender_display_name': None, 'content': None, 'room_id': '111', 'prio': 'high', 'unread': 1, 'missed_calls': None}
            print("base_request_body: %s, body['data']: %s" % (self.base_request_body, data))

            if 'body' in n.content:
                msg_content = n.content['body']
            else:
                msg_content = 'no content, please set up pusher\'s format'
            for retry_number in range(0, MAX_TRIES):
                # FormData = {"request_id": n.event_id, "to": {"registration_id": pushkeys},  ## 推送设备ID.
                #             "body": {
                #                 "platform": "all",
                #                 "message": {
                #                     "msg_content": msg_content,
                #                     "content_type": "text",
                #                     "title": n.room_name,
                #                     "extras": {
                #                         "event_id": n.event_id,
                #                         "room_id": n.room_id,
                #                         "unread": n.counts.unread,
                #                         "prio": "normal",
                #                         "from": n.sender_display_name
                #                     }
                #                 },
                #                 "options": {
                #                     "time_to_live": 86400,
                #                     "apns_production": False,
                #                     "third_party_channel": {
                #                         "huawei": {
                #                             "distribution_new": "mtpush_pns",
                #                             "importance": "NORMAL",
                #                             "category": "IM"
                #                         }
                #                     }
                #                 }
                #             }}
                FormData = {
                    "platform": "android",
                    "audience": {
                        "registration_id": pushkeys
                    },
                    "notification": {
                        "android": {
                            "title": n.room_name,
                            "alert": msg_content,
                            # "builder_id": 0,
                            "category": "alarm",
                            # "small_icon": "mtpush_notification_icon",
                            # "large_icon": "mtpush_notification_icon",
                            "extras": {
                                "event_id": n.event_id,
                                "room_id": n.room_id,
                                "unread": n.counts.unread,
                                "prio": "normal",
                                "from": n.sender_display_name,
                                "type": 1 # type = 1表示社交(与前端沟通)
                            },
                            "priority": 1,
                            "alert_type": 7,
                            "sound": "coin",
                            # "channel_id": "money",
                            # "badge_add_num": 1,
                            # "badge_class": "com.engagelab.app.activity.MainActivity",
                            # "style": 2,
                            # "big_text": "党的十八大提出，倡导富强、民主、文明、和谐，倡导自由、平等、公正、法治，倡导爱国、敬业、诚信、友善，积极培育和践行社会主义核心价值观。富强、民主、文明、和谐是国家层面的价值目标，自由、平等、公正、法治是社会层面的价值取向，爱国、敬业、诚信、友善是公民个人层面的价值准则，这 24 个字是社会主义核心价值观的基本内容。",
                            # "inbox": {
                            #     "inbox1": "this is inbox one",
                            #     "inbox2": "this is inbox two",
                            #     "inbox3": "this is inbox three"
                            # },
                            # "big_pic_path": "https://ss1.bdstatic.com/70cFuXSh_Q1YnxGkpoWK1HF6hhy/it/u=96071541,1913562332&fm=26&gp=0.jpg",
                            # "intent": {
                            #     "url": "intent:#Intent;component=com.engagelab.oaapp/com.engagelab.app.component.UserActivity400;end"
                            # }
                            "intent": {
                                "url": "intent:#Intent;component=" + self.app_key + "/com.aplink.flutter_wallet_pptoken.MainActivity;end"
                            }
                        }
                    },
                    "options": {
                        "third_party_channel": {
                            "vivo": {
                                "classification": 1,
                                "pushMode": 1
                            },
                            "huawei": {
                                "distribution_new": "mtpush_pns",
                                "importance": "NORMAL",
                                "category": "IM"
                            }
                        }
                    }
                }
                if len(pushkeys) == 1:
                    body["to"] = pushkeys[0]
                else:
                    body["registration_ids"] = pushkeys

                log.info("Sending (attempt %i) => %r", retry_number, pushkeys)

                try:
                    span_tags = {"retry_num": retry_number}

                    with self.sygnal.tracer.start_span(
                            "engage_dispatch_try", tags=span_tags, child_of=span_parent
                    ) as span:
                        new_failed, new_pushkeys = await self._request_dispatch(
                            n, log, FormData, headers, pushkeys, span
                        )
                    pushkeys = new_pushkeys
                    failed += new_failed

                    if len(pushkeys) == 0:
                        break
                except TemporaryNotificationDispatchException as exc:
                    retry_delay = RETRY_DELAY_BASE * (2 ** retry_number)
                    if exc.custom_retry_delay is not None:
                        retry_delay = exc.custom_retry_delay

                    log.warning(
                        "Temporary failure, will retry in %d seconds",
                        retry_delay,
                        exc_info=True,
                    )

                    span_parent.log_kv(
                        {"event": "temporary_fail", "retrying_in": retry_delay}
                    )

                    await twisted_sleep(
                        retry_delay, twisted_reactor=self.sygnal.reactor
                    )

            if len(pushkeys) > 0:
                log.info("Gave up retrying reg IDs: %r", pushkeys)
            # Count the number of failed devices.
            span_parent.set_tag("engage_num_failed", len(failed))
            return failed

    @staticmethod
    def _build_data(n: Notification, device: Device) -> Optional[Dict[str, Any]]:
        """
        Build the payload data to be sent.
        Args:
            n: Notification to build the payload for.
            device: Device information to which the constructed payload
            will be sent.

        Returns:
            JSON-compatible dict or None if the default_payload is misconfigured
        """
        data = {}

        if device.data:
            default_payload = device.data.get("default_payload", {})
            if isinstance(default_payload, dict):
                data.update(default_payload)
            else:
                logger.warning(
                    "default_payload was misconfigured, this value must be a dict."
                )
                return None

        for attr in [
            "event_id",
            "type",
            "sender",
            "room_name",
            "room_alias",
            "membership",
            "sender_display_name",
            "content",
            "room_id",
        ]:
            if hasattr(n, attr):
                data[attr] = getattr(n, attr)
                # Truncate fields to a sensible maximum length. If the whole
                # body is too long, ENGAGE will reject it.
                if data[attr] is not None and len(data[attr]) > MAX_BYTES_PER_FIELD:
                    data[attr] = data[attr][0:MAX_BYTES_PER_FIELD]

        data["prio"] = "high"
        if n.prio == "low":
            data["prio"] = "normal"

        if getattr(n, "counts", None):
            data["unread"] = n.counts.unread
            data["missed_calls"] = n.counts.missed_calls

        return data

    def get_jiguang_authorization_header(self, app_key) -> str:
        master_secret = '24df3fd204bb0fa025b4e838'
        base64_str = self.get_authoration(app_key, master_secret)
        return 'Basic ' + base64_str

    def get_authoration(self, app_key, master_secret) -> str:
        origin_str = str.format('{0}:{1}', app_key, master_secret)
        origin_str = origin_str.encode('utf-8')
        base64_str_bytes = base64.b64encode(origin_str)
        base64_str = base64_str_bytes.decode()
        print('base64 str: %s' % base64_str)
        return base64_str
