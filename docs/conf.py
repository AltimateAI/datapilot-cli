extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.extlinks",
    "sphinx.ext.ifconfig",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
]
source_suffix = ".rst"
master_doc = "index"
project = "datapilot-cli"
year = "2024"
author = "Altimate Inc."
copyright = f"{year}, {author}"
version = release = "0.0.14"

pygments_style = "trac"
templates_path = ["."]
extlinks = {
    "issue": ("https://github.com/AltimateAI/datapilot-cli/issues/%s", "#"),
    "pr": ("https://github.com/AltimateAI/datapilot-cli/pull/%s", "PR #"),
}
html_theme = "sphinx_rtd_theme"
# html_theme_path = [furo.get_html_theme_path()]
# html_theme_options = {
#     "githuburl": "https://github.com/AltimateAI/datapilot-cli/",
# }

html_use_smartypants = True
html_last_updated_fmt = "%b %d, %Y"
html_split_index = False
html_sidebars = {
    "**": ["searchbox.html", "globaltoc.html", "sourcelink.html"],
}
html_short_title = f"{project}-{version}"

napoleon_use_ivar = True
napoleon_use_rtype = False
napoleon_use_param = False
