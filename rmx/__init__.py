from ._version import __version__  # NOQA
import colorlog

# NOTE: Here's the default color for colorlog
# The default colors to use for the debug levels
# default_log_colors = {
#     "DEBUG": "white",
#     "INFO": "green",
#     "WARNING": "yellow",
#     "ERROR": "red",
#     "CRITICAL": "bold_red",
# }

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)s:%(name)s: %(message)s',
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'bold_red',
    }
))


logger = colorlog.getLogger('rmx')
logger.addHandler(handler)
