"""Stream type classes for tap-sailthru."""

import copy
import csv
import time
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, Optional

import backoff
import pendulum
import requests
from requests.exceptions import ChunkedEncodingError
from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_error import SailthruClientError
from sailthru.sailthru_response import SailthruResponse
from urllib3.exceptions import MaxRetryError

from tap_sailthru.client import SailthruStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class SailthruJobTimeoutError(Exception):
    """class for sailthru job timeout error.

    Args:
        Exception: error
    """
    pass


class SailthruJobStream(SailthruStream):
    """sailthru stream class."""

    @backoff.on_exception(backoff.expo, SailthruClientError, max_tries=4, factor=3)
    def get_job_url(
        self, client: SailthruClient, job_id: str, timeout: int = 1200
    ) -> str:
        """Poll the /job endpoint and checks to see if export job is completed.

        :param client: SailthruClient, the Sailthru API client
        :param job_id: str, the job_id to poll
        :param timeout: int, number of seconds before halting request (default 1200)
        :returns: str, the export URL
        """
        status = ""
        job_start_time = pendulum.now()
        while status != "completed":
            response = client.api_get("job", {"job_id": job_id}).get_body()
            status = response.get("status")
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Job report status: {status}")
            now = pendulum.now()
            if (now - job_start_time).seconds > timeout:
                # pylint: disable=logging-fstring-interpolation
                self.logger.critical(
                    f"Request with job_id {job_id}"
                    f" exceeded {timeout} second timeout"
                    f"latest_status: {status}"
                )
                raise SailthruJobTimeoutError
            time.sleep(1)
        return response.get("export_url")

    @backoff.on_exception(backoff.expo, ChunkedEncodingError, max_tries=3, factor=2)
    def process_job_csv(
        self, export_url: str, chunk_size: int = 1024, parent_params: dict = None
    ) -> Iterable[dict]:
        """Fetch CSV from URL and streams each line.

        :param export_url: str, The URL from which to fetch the CSV data from
        :param chunk_size: int, The chunk size to read per line
        :param parent_params: dict, A dictionary with "parent" parameters to append
            to each record
        :returns: Iterable[dict], A generator of a dictionary
        """
        with requests.get(export_url, stream=True) as req:
            try:
                reader = csv.reader(
                    (
                        line.decode("utf-8")
                        for line in req.iter_lines(chunk_size=chunk_size)
                    ),
                    delimiter=",",
                    quotechar='"',
                )
                fieldnames = next(reader)
                for row in reader:
                    dicted_row = {}
                    for n, v in zip(fieldnames, row):
                        if n not in dicted_row.keys():
                            dicted_row[n] = v
                    if parent_params:
                        dicted_row.update(parent_params)
                    transformed_row = self.post_process(dicted_row)
                    if transformed_row is None:
                        continue
                    else:
                        yield self.post_process(dicted_row)
            except ChunkedEncodingError:
                self.logger.error(
                    "Chunked Encoding Error in the list member stream, stopping early"
                )
                pass


class AccountsStream(SailthruStream):
    """Define custom stream."""

    name = "accounts"
    path = "settings"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "accounts.json"

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response["account_name"] = self.config.get("account_name")
        yield response

    def post_process(self, row: str, context: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        k_arr = row["domains"].copy().keys()
        for k in k_arr:
            if k == "":
                del row["domains"][k]
        return row


class BlastStream(SailthruStream):
    """Define custom stream."""

    name = "blasts"
    path = "blast"
    primary_keys = ["blast_id"]
    replication_key = "start_time"
    schema_filepath = SCHEMAS_DIR / "blasts.json"

    def get_url(self, context: Optional[dict]) -> str:
        """Construct url for api request.

        Args:
            context (Optional[dict]): meltano context dict

        Returns:
            str: url
        """
        return self.path

    def get_records(self, context: dict | None):
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        # Must query by status. https://getstarted.sailthru.com/developers/api/blast/
        all_statuses = ["sent"]
        for status in all_statuses:
            self.blast_stream_query_status = status
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        :param context: Stream partition or context dictionary.
        :returns: dict, A dictionary containing the request payload.
        """
        return {
            "status": self.blast_stream_query_status,
            "limit": 0,
            "start_date": self.stream_state.get("starting_replication_value"),
        }

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        Args:
            record (dict): Individual record in the stream.
            context (Optional[dict]): Stream partition or context dictionary

        Returns:
            dict: dictionary for child stream
        """
        return {"blast_id": record["blast_id"]}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["blasts"]:
            row["account_name"] = self.config.get("account_name")
            yield row

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = copy.deepcopy(row)
        new_row.pop("start_time")
        start_time = row["start_time"]
        parsed_start_time = pendulum.from_format(
            start_time, "ddd, D MMM YYYY HH:mm:ss ZZ"
        )
        new_row["start_time"] = parsed_start_time.to_datetime_string()
        return new_row


class BlastStatsStream(SailthruStream):
    """Define custom stream."""

    name = "blast_stats"
    path = "stats"
    primary_keys = ["blast_id"]
    schema_filepath = SCHEMAS_DIR / "blast_stats.json"
    parent_stream_type = BlastStream
    rest_method = "GET"

    def get_url(self, context: Optional[dict]) -> str:
        """Construct url for api request.

        Args:
            context (Optional[dict]): meltano context dict

        Returns:
            str: url
        """
        return self.path

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        :param context: Stream partition or context dictionary
        :returns: dict, A dictionary containing the request payload
        """
        return {
            "stat": "blast",
            "blast_id": context["blast_id"],
            "beacon_times": 1,
            "click_times": 1,
            "clickmap": 1,
            "domain": 1,
            "engagement": 1,
            "purchase_times": 1,
            "signup": 1,
            "subject": 1,
            "topusers": 1,
            "urls": 1,
            "banners": 1,
            "purchase_items": 1,
            "device": 1,
        }

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response["account_name"] = self.config.get("account_name")
        response["blast_id"] = context["blast_id"]
        yield response

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = row.copy().keys()
        if "beacon_times" in keys_arr:
            beacon_times_dict = row.copy()["beacon_times"]
            new_beacon_times_arr = []
            for k, v in beacon_times_dict.items():
                new_beacon_times_arr.append({"beacon_time": k, "count": v})
            row["beacon_times"] = new_beacon_times_arr
        if "click_times" in keys_arr:
            click_times_dict = row.copy()["click_times"]
            new_click_times_arr = []
            for k, v in click_times_dict.items():
                new_click_times_arr.append({"click_time": k, "count": v})
            row["click_times"] = new_click_times_arr
        if "domain" in keys_arr:
            domain_stats_dict = row.pop("domain")
            new_domain_stats_arr = []
            for domain, domain_dict in domain_stats_dict.items():
                new_domain_stats_arr.append({**{"domain": domain}, **domain_dict})
            row["domain_stats"] = new_domain_stats_arr
        if "signup" in keys_arr:
            signup_stats_dict = row.pop("signup")
            new_signup_stats_arr = [
                signup_dict for signup_dict in signup_stats_dict.values()
            ]
            row["signup"] = new_signup_stats_arr
        if "subject" in keys_arr:
            subject_stats_dict = row.pop("subject")
            new_subject_stats_arr = []
            for subject, subject_dict in subject_stats_dict.items():
                new_subject_stats_arr.append({**{"subject": subject}, **subject_dict})
            row["subject"] = new_subject_stats_arr
        if "urls" in keys_arr:
            url_stats_dict = row.pop("urls")
            new_url_stats_arr = []
            for url, url_dict in url_stats_dict.items():
                new_url_stats_arr.append({**{"url": url}, **url_dict})
            row["urls"] = new_url_stats_arr
        if "device" in keys_arr:
            device_stats_dict = row.pop("device")
            new_device_stats_arr = []
            for device, device_dict in device_stats_dict.items():
                new_device_stats_arr.append({**{"device": device}, **device_dict})
            row["device_stats"] = new_device_stats_arr
        return row


class BlastQueryStream(SailthruJobStream):
    """Custom Stream for the results of a Blast Query job."""
    name = "blast_query"
    job_name = "blast_query"
    path = "job"
    primary_keys = ["profile_id", "blast_id"]
    schema_filepath = SCHEMAS_DIR / "blast_query.json"
    parent_stream_type = BlastStream

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {"job": "blast_query", "blast_id": context["blast_id"]}

    def get_records(self, context: Optional[dict]):
        """Retrieve records by creating and processing export job.

        Args:
            context (Optional[dict]): Stream partition or context dictionary

        Yields:
            dict: Individual record from stream
        """
        blast_id = context["blast_id"]
        client = self.authenticator
        payload = self.prepare_request_payload(context=context)
        response = client.api_post("job", payload).get_body()
        if response.get("error"):
            # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
            # Error code 99 = You may not export a blast that has been sent
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Skipping blast_id: {blast_id}")
            return
        try:
            export_url = self.get_job_url(client=client, job_id=response["job_id"])
        except MaxRetryError:
            self.logger.info(f"Skipping blast_id: {blast_id}")
            return

        # Add blast id to each record
        yield from self.process_job_csv(
            export_url=export_url,
            parent_params={
                "blast_id": blast_id,
                "account_name": self.config.get("account_name"),
            },
        )

    def post_process(self, row: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = {}
        for k, v in row.items():
            new_row[k.lower().replace(" ", "_")] = v
        return new_row


class TemplateStream(SailthruStream):
    """Custom Stream for templates."""

    name = "templates"
    path = "template"
    primary_keys = ["template_id"]
    schema_filepath = SCHEMAS_DIR / "templates.json"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["templates"]:
            row["account_name"] = self.config.get("account_name")
            yield row


class ListStream(SailthruStream):
    """Custom Stream for lists."""

    name = "lists"
    path = "list"
    primary_keys = ["list_id"]
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        Args:
            record (dict): Individual record from the stream
            context (Optional[dict]): Stream partition or context dictionary

        Returns:
            dict: dictionary with context for the child stream
        """
        return {"list_id": record["list_id"], "list_name": record["name"]}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["lists"]:
            row["account_name"] = self.config.get("account_name")
            yield row


class PrimaryListStream(ListStream):
    """Custom Stream for lists."""

    name = "primary_lists"
    path = "list"
    primary_keys = ["list_id"]
    replication_key = "create_time"
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {"primary": 1}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        Args:
            record (dict): Individual record from the stream
            context (Optional[dict]): Stream partition or context dictionary

        Returns:
            dict: dictionary with context for the child stream
        """
        return {"list_id": record["list_id"], "list_name": record["name"]}

    def parse_response(
        self, response: SailthruResponse, context: Optional[dict]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for row in response["lists"]:
            row["account_name"] = self.config.get("account_name")
            yield row


class ListStatsStream(SailthruStream):
    """Define custom stream."""

    name = "list_stats"
    path = "stats"
    primary_keys = ["list_id"]
    schema_filepath = SCHEMAS_DIR / "list_stats.json"
    parent_stream_type = ListStream
    rest_method = "GET"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {
            "stat": "list", "list": context["list_name"]
        }

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response["account_name"] = self.config.get("account_name")
        response["list_id"] = context["list_id"]
        yield response

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = row.copy().keys()
        if "signup_month" in keys_arr:
            signup_month_stats_dict = row.pop("signup_month")
            new_signup_month_stats_arr = []
            for signup_month, signup_month_dict in signup_month_stats_dict.items():
                new_signup_month_stats_arr.append(
                    {**{"signup_month": signup_month}, **signup_month_dict}
                )
            row["signup_month"] = new_signup_month_stats_arr
        if "source_count" in keys_arr:
            source_count_dict = row.copy()["source_count"]
            new_source_count_arr = []
            for k, v in source_count_dict.items():
                new_source_count_arr.append({"source": k, "count": v})
            row["source_count"] = new_source_count_arr
        if "source_signup_count" in keys_arr:
            source_signup_count_dict = row.copy()["source_signup_count"]
            new_source_signup_count_arr = []
            try:
                for k, v in source_signup_count_dict.items():
                    new_source_signup_count_arr.append({"source": k, "count": v})
                row["source_signup_count"] = new_source_signup_count_arr
            except AttributeError:
                row["source_signup_count"] = []
        return row


class ListMemberStream(SailthruJobStream):
    """Custom Stream for the results of a Export List Data job."""
    name = "list_members"
    job_name = "list_members"
    path = "job"
    schema_filepath = SCHEMAS_DIR / "list_members.json"
    parent_stream_type = ListStream

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dictionary containing the request payload.
        """
        return {"job": "export_list_data", "list": context["list_name"]}

    def get_records(self, context: Optional[dict]):
        """Retrieve records by creating and processing export job.

        Args:
            context (Optional[dict]): Stream partition or context dictionary

        Yields:
            dict: Individual record from stream
        """
        list_name = context["list_name"]
        list_id = context["list_id"]
        client = self.authenticator
        payload = self.prepare_request_payload(context=context)
        response = client.api_post("job", payload).get_body()
        if response.get("error"):
            # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
            # Error code 99 = You may not export a blast that has been sent
            # pylint: disable=logging-fstring-interpolation
            self.logger.info(f"Skipping list_name: {list_name}")
            return
        try:
            export_url = self.get_job_url(
                client=client,
                job_id=response["job_id"],
                timeout=self.config.get('request_timeout')
            )
        except MaxRetryError:
            self.logger.info(f"Skipping list: {list_name}")
            return

        # Add list id to each record
        yield from self.process_job_csv(
            export_url=export_url,
            parent_params={
                "list_name": list_name,
                "list_id": list_id,
                "account_name": self.config.get("account_name"),
            },
        )

    def post_process(self, row: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        new_row = {}
        # We convert the row into an ordered dict to prioritize Sailthru-native vars
        # over user-created vars with the same name
        ordered_row = OrderedDict(row)
        schema_keys = list(self._schema.copy()["properties"].keys())
        custom_vars_arr = []
        for k, v in ordered_row.items():
            cleaned_key = k.lower().replace(" ", "_")
            if cleaned_key in schema_keys and cleaned_key not in new_row.keys():
                new_row[cleaned_key] = v
            else:
                custom_vars_arr.append({"var_name": k, "var_value": str(v)})
        new_row["custom_vars"] = custom_vars_arr
        if "email_hash" not in new_row.keys():
            return None
        return new_row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"list_id": record["list_id"], "profile_id": record["profile_id"]}


class UsersStream(SailthruStream):
    """Define custom stream."""

    name = "users"
    path = "user"
    primary_keys = ["email"]
    schema_filepath = SCHEMAS_DIR / "users.json"
    parent_stream_type = ListMemberStream
    state_partitioning_keys = []

    def get_url(self, context: Optional[dict]) -> str:
        return self.path

    @backoff.on_exception(backoff.expo,
                          TimeoutError,
                          max_tries=5,
                          factor=4)
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.
        If pagination is detected, pages will be recursed automatically.
        Args:
            context: Stream partition or context dictionary.
        Yields:
            An item for every record in the response.
        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token= None
        finished = False

        client = self.authenticator
        http_method = self.rest_method
        url: str = self.get_url(context)
        request_data = self.prepare_request_payload(context, next_page_token) or {}
        headers = self.http_headers

        while not finished:
            try:
                request = client._api_request(
                    url,
                    request_data,
                    http_method,
                    headers=headers
                )
                resp = request.get_body()
                yield from self.parse_response(resp, context=context)
                previous_token = copy.deepcopy(next_page_token)
                next_page_token = self.get_next_page_token(
                    response=resp, previous_token=previous_token
                )
                if next_page_token and next_page_token == previous_token:
                    raise RuntimeError(
                        f"Loop detected in pagination. "
                        f"Pagination token {next_page_token} is identical to prior token."
                    )
                # Cycle until get_next_page_token() no longer returns a value
                finished = not next_page_token
            except SailthruClientError:
                self.logger.info(f"SailthruClientError for User ID : {request_data['id']}")
                pass

    def prepare_request_payload(
        self,
        context: Optional[dict],
        next_page_token: Optional[str] = None
    ) -> dict:
        """Prepare request payload.
        Args:
            context: Stream partition or context dictionary.
        Returns:
            A dictionary containing the request payload.
        """
        sid = context['profile_id']
        return {
            'id': sid,
            'key': 'sid',
        }

    def parse_response(self, response: dict, context: Optional[dict]) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield response

    def post_process(self, row: dict, context: dict) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        keys_arr = list(row.copy().keys())
        if "lists" in keys_arr:
            lists_arr = []
            lists_dict = row.pop("lists")
            if lists_dict:
                for k, v in lists_dict.items():
                    lists_arr.append({"list_name": k, "signup_time": v})
            row["lists"] = lists_arr
        return row
