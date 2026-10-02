import json
import time
from typing import List, Optional
import PIL
from PIL import Image
from pydantic import BaseModel
import openai
from openai import APITimeoutError, RateLimitError, BadRequestError

from marker.logger import get_logger
from marker.schema.blocks import Block
from marker.services.openai import OpenAIService

logger = get_logger()


class OptimizedOpenAIService(OpenAIService):
    """
    客製化 OpenAI 服務：
    繼承自 Marker 原生 OpenAIService，具備以下增強：
    1. reasoning_effort 控制 (如 'low', 'none', 'medium')，避免 Reasoning 模型產生過長思維鏈導致超時與浪費資源。
    2. enable_thinking 控制 (支援 vLLM/Qwen 等模型的 chat_template_kwargs: {'thinking': False})。
    3. 自動降級保護 (Fallback)：若後端不支援 reasoning_effort 或 extra_body 參數，自動剔除並平滑重試。
    4. 完全相容 Marker 的依賴注入與設定機制 (ConfigParser / assign_config)。
    """
    reasoning_effort: str = "low"
    enable_thinking: bool = False

    def __call__(
        self,
        prompt: str,
        image: PIL.Image.Image | List[PIL.Image.Image] | None,
        block: Block | None,
        response_schema: type[BaseModel],
        max_retries: int | None = None,
        timeout: int | None = None,
    ):
        if max_retries is None:
            max_retries = self.max_retries

        if timeout is None:
            timeout = self.timeout

        client = self.get_client()
        image_data = self.format_image_for_llm(image)

        messages = [
            {
                "role": "user",
                "content": [
                    *image_data,
                    {"type": "text", "text": prompt},
                ],
            }
        ]

        parse_kwargs = {
            "extra_headers": {
                "X-Title": "Marker",
                "HTTP-Referer": "https://github.com/datalab-to/marker",
            },
            "model": self.openai_model,
            "messages": messages,
            "timeout": timeout,
            "response_format": response_schema,
        }

        # 思考等級控制 (reasoning_effort: low, medium, high)
        effort = (str(self.reasoning_effort) if self.reasoning_effort is not None else "").strip().lower()
        if effort and effort not in ("default", "none", "off", "false"):
            parse_kwargs["reasoning_effort"] = effort

        # 思考開關 (thinking template kwargs)
        # 若明確關閉思考或設定為 none/off，注入 chat_template_kwargs: {'thinking': False}
        thinking_allowed = bool(self.enable_thinking) and effort not in ("none", "off", "false")
        if not thinking_allowed:
            parse_kwargs["extra_body"] = {"chat_template_kwargs": {"thinking": False}}

        total_tries = max_retries + 1
        for tries in range(1, total_tries + 1):
            try:
                response = client.chat.completions.parse(**parse_kwargs)
                response_text = response.choices[0].message.content
                total_tokens = response.usage.total_tokens if response.usage else 0
                if block:
                    block.update_metadata(
                        llm_tokens_used=total_tokens, llm_request_count=1
                    )
                return json.loads(response_text)
            except BadRequestError as e:
                # 若後端 server 不支援 reasoning_effort 或 extra_body，降級重試
                err_msg = str(e).lower()
                modified = False
                if "reasoning_effort" in parse_kwargs and ("reasoning_effort" in err_msg or "extra_body" in err_msg or "unrecognized" in err_msg):
                    logger.warning(f"後端不支援 reasoning_effort，自動移除降級: {e}")
                    del parse_kwargs["reasoning_effort"]
                    modified = True
                if "extra_body" in parse_kwargs and ("chat_template_kwargs" in err_msg or "extra_body" in err_msg or "unrecognized" in err_msg):
                    logger.warning(f"後端不支援 extra_body/thinking 控制，自動移除降級: {e}")
                    del parse_kwargs["extra_body"]
                    modified = True
                if modified:
                    continue
                logger.error(f"OpenAI BadRequestError failed: {e}")
                break
            except (APITimeoutError, RateLimitError) as e:
                if tries == total_tries:
                    logger.error(
                        f"Rate limit / timeout error: {e}. Max retries reached. Giving up. (Attempt {tries}/{total_tries})",
                    )
                    break
                else:
                    wait_time = tries * self.retry_wait_time
                    logger.warning(
                        f"Rate limit / timeout error: {e}. Retrying in {wait_time} seconds... (Attempt {tries}/{total_tries})",
                    )
                    time.sleep(wait_time)
            except Exception as e:
                logger.error(f"OpenAI inference failed: {e}")
                break

        return {}
