from textwrap import dedent

from .default import DEFAULT_TEMPLATE
from .ner_keyword_extractor_template import NER_KEYWORD_EXTRACT_TEMPLATE
from .multiple_references_template import MULTIPLE_REFERENCES_TEMPLATE
from .direct_chat import DIRECT_CHAT_TEMPLATE

PROMPT_TEMPLATES = {
    "default": dedent(DEFAULT_TEMPLATE).strip(),
    "ner_keyword_extractor": dedent(NER_KEYWORD_EXTRACT_TEMPLATE).strip(),
    "multiple_references": dedent(MULTIPLE_REFERENCES_TEMPLATE).strip(),
    "direct_chat": dedent(DIRECT_CHAT_TEMPLATE).strip(),
}

def get_prompt_template(template_name: str) -> str:
    try:
        return PROMPT_TEMPLATES[template_name]
    except KeyError:
        raise ValueError(f"Invalid template name: {template_name}")