from tabulate import tabulate

from datapilot.core.insights.schema import Severity

RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
NO_COLOR = "\033[0m"

BOLD = "\033[1m"
UNDERLINE = "\033[4m"
ITALIC = "\033[3m"


def numbered_list(items):
    return "\n".join(f"{i}. {item}" for i, item in enumerate(items, 1))


def get_severity_color(severity: Severity):
    colors = {
        Severity.ERROR: RED,
        Severity.WARNING: YELLOW,
        Severity.INFO: BLUE,
    }
    return colors.get(severity, NO_COLOR)  # Default to no color


def reset_color(do_format=True):
    return NO_COLOR if do_format else ""


def bold(text, do_format=True):
    return f"{BOLD if do_format else ''}{text}{NO_COLOR if do_format else ''}"


def underline(text, do_format=True):
    return f"{UNDERLINE if do_format else ''}{text}{NO_COLOR if do_format else ''}"


def italic(text, do_format=True):
    return f"{ITALIC if do_format else ''}{text}{NO_COLOR if do_format else ''}"


def tabulate_data(data, headers, tablefmt="grid"):
    return tabulate(
        data,
        headers=headers,
        tablefmt=tablefmt,
        maxcolwidths=[None, None, None, 60, 60, 60],
    )


def color_text(text, color):
    return f"{color}{text}{NO_COLOR}"


def color_based_on_severity(severity: Severity):
    color = get_severity_color(severity)
    return color_text(severity.value, color)
