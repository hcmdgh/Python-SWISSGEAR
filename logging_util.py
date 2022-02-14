from .datetime_util import datetime2str as _datetime2str
import typing
import datetime


class _SmartFilePointer:
    """
    可以自动close的文件指针对象。
    """
    def __init__(self, file_path: str, mode: str):
        self.fp = open(file_path, mode, encoding='utf-8')

    def __del__(self):
        self.fp.close()


_use_stdout: bool = True
_file_pointer: typing.Optional[_SmartFilePointer] = None


def _write_log(level: str, msg: str):
    now = _datetime2str(datetime.datetime.now())
    log = f"{now} [{level}] {msg}"

    if _use_stdout:
        print(log)

    if _file_pointer:
        print(log, file=_file_pointer.fp, flush=True)


def set_stream(file_path: typing.Optional[str] = None,
               overwrite: bool = True,
               use_stdout: bool = True):
    """
    设置是否将日志信息输出到控制台和文件。
    """
    global _file_pointer, _use_stdout

    _use_stdout = use_stdout

    if file_path:
        if overwrite:
            _file_pointer = _SmartFilePointer(file_path, 'w')
        else:
            _file_pointer = _SmartFilePointer(file_path, 'a')
    else:
        _file_pointer = None
        
        
def debug(msg: str):
    _write_log(level='DEBUG', msg=msg)


def info(msg: str):
    _write_log(level='INFO', msg=msg)


def error(msg: str):
    _write_log(level='ERROR', msg=msg)


def warn(msg: str):
    _write_log(level='WARN', msg=msg)


def warning(msg: str):
    return warn(msg=msg)
