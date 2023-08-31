"""url"""
from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urljoin, urlparse


def set_url_parameters(url, params: dict[str, str]):
    """change url parameters"""

    # parse url and replace query
    parsed = urlparse(url)
    parsed = parsed._replace(query=urlencode(dict(params)))

    return parsed.geturl()


def append_parameters_to_url(url, params: dict[str, str]):
    """add url parameters"""

    # parse url and update parameters
    parsed = urlparse(url)
    params = {**dict(parse_qsl(parsed.query)), **params}

    return set_url_parameters(url, params=params)


def append_path_to_url(url, *args: tuple[str]):
    """add path to existing path"""

    # parse url
    parsed = urlparse(url)

    # get path element and join
    args = [parsed.path] + list(args)
    path = "/".join(map(lambda x: str(x).rstrip("/"), args))

    # replace path
    parsed = parsed._replace(path=path)

    return parsed.geturl()


def make_myc_url(
    url: str,
    scenario_ids: list[int],
    path: str | None = None,
    params: dict[str, str] | None = None,
) -> str:
    """make myc url"""

    # make base url
    url = urljoin(url, ",".join(map(str, scenario_ids)))

    # add path
    if path is not None:
        url = append_path_to_url(url, path)

    # add parameters
    if params is not None:
        url = set_url_parameters(url, params)

    return url
