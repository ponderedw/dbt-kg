import os

from langchain_openai import ChatOpenAI as BaseChatOpenAI


class ChatOpenAI(BaseChatOpenAI):
    """A wrapper for the `langchain_aws.ChatOpenAI`."""

    def __init__(self, model_id: str = None, **kwargs):
        """Initialize the `ChatOpenAI` with specific configuration."""
        model_type, model_id_env = os.environ['LLM_MODEL_ID'].split(':', 1)
        default_kwargs = {
            'model': model_id or model_id_env,
            'temperature': 0,
        }

        super().__init__(**(default_kwargs | kwargs))
