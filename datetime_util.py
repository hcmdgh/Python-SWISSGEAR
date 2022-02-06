import datetime
import typing


class ParseError(RuntimeError):
    """
    当日期字符串无法解析时抛出的异常。
    """
    pass


def datetime2str(dt: typing.Union[datetime.date, datetime.datetime], *, lang: str = 'en') -> str:
    """
    将date或datetime类型转换为“年月日时分秒”格式的字符串。

    可以指定输出语言为中文或英文，形如“2022年02月06日 18时46分27秒”和“2022-02-06 18:46:27”。
    """
    if lang == 'en':
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    elif lang in ('cn', 'zh'):
        return dt.strftime("%Y年%m月%d日 %H时%M分%S秒")
    else:
        raise AssertionError


def date2str(dt: typing.Union[datetime.date, datetime.datetime], *, lang: str = 'en') -> str:
    """
    将date或datetime类型转换为“年月日”格式的字符串。

    可以指定输出语言为中文或英文，形如“2022年02月06日”和“2022-02-06”。
    """
    if lang == 'en':
        return dt.strftime("%Y-%m-%d")
    elif lang in ('cn', 'zh'):
        return dt.strftime("%Y年%m月%d日")
    else:
        raise AssertionError


def str2datetime(s: str, *, custom_format: str = '') -> datetime.datetime:
    """
    将字符串形式的日期时间转换为datetime类型，支持的格式如下：

    1. %Y-%m-%d %H:%M:%S
    2. %Y/%m/%d %H:%M:%S
    3. %Y-%m-%d
    4. %Y/%m/%d
    5. %H:%M:%S
    6. %Y年%m月%d日 %H时%M分%S秒
    7. %Y年%m月%d日 %H:%M:%S
    8. %Y年%m月%d日

    也可以自定义字符串格式。
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%H:%M:%S",
        "%Y年%m月%d日 %H时%M分%S秒",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y年%m月%d日",
    ]

    if custom_format:
        formats.insert(0, custom_format)

    for f in formats:
        try:
            return datetime.datetime.strptime(s, f)
        except ValueError:
            pass

    raise ParseError


def ensure_datetime(dt: typing.Union[datetime.date, datetime.datetime]) -> datetime.datetime:
    """
    将date类型或datetime类型统一转换为datetime类型。
    """
    if type(dt) == datetime.date:
        return datetime.datetime.combine(dt, datetime.datetime.min.time())
    elif type(dt) == datetime.datetime:
        return dt
    else:
        raise AssertionError
