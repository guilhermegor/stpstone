import logging
import os
import time
import inspect


class CreateLog:

    def creating_parent_folder(self, new_path):
        if not os.path.exists(new_path):
            os.makedirs(new_path)
            return True
        else:
            return False

    def basic_conf(self, complete_path=None, basic_level='info'):
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

    def infos(self, logger, msg_str):
        return logger.info(msg_str)

    def warnings(self, logger, msg_str):
        return logger.warning(msg_str)

    def errors(self, logger, msg_str):
        return logger.error(msg_str, exc_info=True)

    def critical(self, logger, msg_str):
        return logger.error(msg_str)

    def log_message(self, logger, message: str, log_level: str = "infos") -> None:
        """
        Unified logging method that works with all CreateLog levels.

        Args:
            message: The log message
            log_level: One of 'infos', 'warnings', 'errors', 'critical' (matches CreateLog methods)

        Returns:
            None
        """
        class_name = self.__class__.__name__
        method_name = inspect.currentframe().f_back.f_code.co_name
        formatted_message = f"[{class_name}.{method_name}] {message}"
        if logger is not None:
            log_method = getattr(self, log_level, self.infos)
            log_method(logger, formatted_message)
        else:
            level = log_level.upper() if log_level != 'infos' else 'INFO'
            timestamp = f"{time.strftime('%Y-%m-%d,%H:%M:%S')}.{int(time.time() * 1000) % 1000:03d}"
            print(f"{timestamp} {level} {{{class_name}}} [{method_name}] {message}")


# decorators
def timeit(method):
    """
    REFERENCES: https://medium.com/pythonhive/python-decorator-to-measure-the-execution-time-of-methods-fa04cb6bb36d
    DOCSTRING: TIMING DECORRATOR TO MEASURE ELAPSED TIME TO EXECUTE A FUNCTION
    INPUTS: -
    OUTPUTS: ELAPSED TIME PRINTED
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' %
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed

def conditional_timeit(bl_use_timer):
    """
    DOCSTRING: APPLIES THE @TIMEIT DECORATOR CONDITIONALLY BASED ON `USE_TIMER`
    INPUT:
        - USE_TIMER: BOOLEAN INDICATING WHETHER TO APPLY TIMING.
    OUTPUT:
        - A FUNCTION WRAPPED WITH THE @TIMEIT DECORATOR IF `USE_TIMER` IS TRUE.
    """
    def decorator(method):
        if bl_use_timer:
            return timeit(method)
        return method
    return decorator
