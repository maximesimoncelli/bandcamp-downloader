import logging
from colorama import Fore, Style, init

init(autoreset=True)

SEVERITY_COLORS = {
    'DEBUG': Fore.CYAN,
    'INFO': Fore.GREEN,
    'WARNING': Fore.YELLOW,
    'ERROR': Fore.RED,
    'CRITICAL': Fore.MAGENTA
}


def logger(message, severity='INFO'):
    color = SEVERITY_COLORS.get(severity.upper(), Fore.WHITE)
    print(f"{color}[{severity.upper()}]{Style.RESET_ALL} {message}")
    # Optionally, also log to file or use logging module
    logging.log(getattr(logging, severity.upper(), logging.INFO), message)
