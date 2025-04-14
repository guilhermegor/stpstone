import logging
import os
import time
import inspect
from typing import Literal, Optional
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class CreateLog(metaclass=TypeChecker):


    def creating_parent_folder(self, new_path: str):
        if not os.path.exists(new_path):
            os.makedirs(new_path)
            return True
        else:
            return False

    def basic_conf(self, complete_path: str, basic_level: str ="info") -> logging.Logger:
        if basic_level == 'info':
            level = logging.INFO
        elif basic_level == 'debug':
            level = logging.DEBUG
        else:
            raise Exception(
                'Level was not properly defined in basic config of logging, please check')
        logging.basicConfig(
            level=level,
            filename=complete_path,
            format=(
                '%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] '
                '%(message)s'
            ),
            datefmt='%Y-%m-%d,%H:%M:%S',
        )
        console = logging.StreamHandler()
        console.setLevel
        logger = logging.getLogger(__name__)
        return logger

    def info(self, logger: Optional[logging.Logger], msg_str: str) -> logging.Logger:
        return logger.info(msg_str)

    def warning(self, logger: Optional[logging.Logger], msg_str: str) -> logging.Logger:
        return logger.warning(msg_str)

    def error(self, logger: Optional[logging.Logger], msg_str: str) -> logging.Logger:
        return logger.error(msg_str, exc_info=True)

    def critical(self, logger: Optional[logging.Logger], msg_str: str) -> logging.Logger:
        return logger.error(msg_str)

    def log_message(self, logger: Optional[logging.Logger], message: str,
                    log_level: Literal["info", "warning", "error", "critical"]) -> None:
        """
        Unified logging method that works with all CreateLog levels.

        Args:
            message: The log message
            log_level: One of 'info', 'warning', 'error', 'critical' (matches CreateLog methods)

        Returns:
            None
        """
        frame = inspect.currentframe()
        class_name = 'UnknownClass'
        method_name = 'unknown_method'
        set_skip_modules = {
            'pydantic',
            'typing',
            'inspect',
            'logging',
            'stpstone.transformations.validation'
        }
        # walk up the call stack to find the first non-CreateLog frame
        while frame:
            frame = frame.f_back
            if not frame:
                break
            module_name = frame.f_globals.get('__name__', 'UnknownModule')
            if any(module_name.startswith(prefix) for prefix in set_skip_modules):
                continue
            self_potential_cls = frame.f_locals.get('self')
            if self_potential_cls is not None and not isinstance(self_potential_cls, CreateLog):
                class_name = self_potential_cls.__class__.__name__
                method_name = frame.f_code.co_name
                break
            # fallback to function name if no suitable self found
            method_name = frame.f_code.co_name
        formatted_message = f"[{class_name}.{method_name}] {message}"
        if logger is not None:
            log_method = getattr(self, log_level, self.locals()[log_level])
            log_method(logger, formatted_message)
        else:
            level = log_level.upper()
            timestamp = f"{time.strftime('%Y-%m-%d,%H:%M:%S')}.{int(time.time() * 1000) % 1000:03d}"
            print(f"{timestamp} {level} {{{class_name}}} [{method_name}] {message}")


def timeit(method: callable) -> callable:
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed

def conditional_timeit(bl_use_timer: bool) -> callable:
    """
    Applies the @timeit decorator conditionally based on `use_timer`

    Args:
        use_timer: boolean indicating whether to apply timing.

    Returns:
        a function wrapped with the @timeit decorator if `use_timer` is true.
    """
    def decorator(method):
        if bl_use_timer:
            return timeit(method)
        return method
    return decorator
