from osbot_utils.base_classes.Type_Safe import Type_Safe

class Prefect__Artifacts(Type_Safe):
    LINKS    : str = "links"
    MARKDOWN : str = "markdown"         # ok: shows in the 'Artifacts' Tab
    PROGRESS : str = "progress"         # todo: check it this one actually works (I was getting 'unknown')
    IMAGES   : str = "images"
    TABLES   : str = "tables"           # todo: also check this one (the prob might be with the structure of the data object)
    RESULT   : str = "result"           # ok: shows in the 'Results' Tab