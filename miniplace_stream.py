"""Assemble Cohere v2 streams without executing partially generated tool calls."""

import copy
import json
from uuid import uuid4

from cohere.v2.types import V2ChatResponse
from jsonschema import Draft202012Validator, ValidationError


class IncompleteModelStream(RuntimeError):
    """A stream did not finish with a usable, complete message."""

class InvalidModelAction(RuntimeError):
    """A complete model response contains a malformed tool call."""


def argument_object(value):
    """Decode formatting wrappers without guessing missing tool arguments."""
    def invalid_constant(value):
        raise ValueError(f"Non-JSON numeric constant: {value}")
    for _ in range(3):
        if not isinstance(value, str):
            break
        value = json.loads(value, parse_constant=invalid_constant)
    if not isinstance(value, dict):
        raise ValueError("Tool arguments must encode a JSON object.")
    json.dumps(value, allow_nan=False)
    return value


def repair_tool_history(messages, quarantined_ids=()):
    """Repair API replay only; retain unreplayable exchanges as quoted data notes."""
    repaired = copy.deepcopy(messages)
    invalid, reports = {}, []
    for index, message in enumerate(repaired):
        if not message.get("tool_calls"):
            continue
        valid = []
        for call in message["tool_calls"]:
            identifier = call.get("id")
            try:
                if identifier in quarantined_ids:
                    raise ValueError("The API rejected this historical call.")
                original = call["function"]["arguments"]
                arguments = argument_object(original)
                if not isinstance(original, str) or not isinstance(json.loads(original), dict):
                    call["function"]["arguments"] = json.dumps(arguments, allow_nan=False)
                    reports.append(dict(call_id=identifier, repair="normalized argument encoding"))
                valid.append(call)
            except (ValueError, TypeError, KeyError) as error:
                invalid[identifier] = dict(message_index=index, call=copy.deepcopy(call), reason=str(error), results=[])
                reports.append(dict(call_id=identifier, repair="quarantined malformed historical exchange", reason=str(error)))
        if valid:
            message["tool_calls"] = valid
        else:
            message.pop("tool_calls", None)
    result = []
    for message in repaired:
        if message.get("role") == "tool" and message.get("tool_call_id") in invalid:
            invalid[message["tool_call_id"]]["results"].append(copy.deepcopy(message))
            continue
        if message.get("role") == "assistant":
            if len(message) == 1:
                continue
            result.extend(conversation_messages(message))
        else:
            result.append(message)
    if invalid:
        note = {"role": "user", "content": json.dumps(dict(
            kind="history_repair",
            note="These historical tool exchanges cannot be replayed as native calls. Their original arguments/results are preserved here for reference. They were NOT re-executed. Continue from current canvas state and use valid tool arguments.",
            exchanges=list(invalid.values()),
        ))}
        position = next((i for i, message in enumerate(result) if message.get("role") in ("assistant", "tool")), max(0, len(result)-1))
        result.insert(position, note)
        if messages and messages[-1].get("role") == "tool" and messages[-1].get("tool_call_id") in invalid:
            try:
                payload = json.loads(messages[-1]["content"])
                if isinstance(payload, dict) and payload.get("live_state"):
                    result.append({"role": "user", "content": json.dumps(payload["live_state"])})
            except (ValueError, TypeError, KeyError):
                pass
    return result, reports


def validate_response_calls(response, tools):
    schemas = {tool["function"]["name"]: tool["function"]["parameters"] for tool in tools}
    for call in response.message.tool_calls or []:
        name = call.function.name
        if name not in schemas:
            raise InvalidModelAction(f"Unknown or unavailable tool: {name}")
        try:
            arguments = argument_object(call.function.arguments)
            Draft202012Validator(schemas[name]).validate(arguments)
            call.function.arguments = json.dumps(arguments, allow_nan=False)
            if not isinstance(call.id, str) or not call.id:
                call.id = f"native-tool-{uuid4().hex}"
        except (ValueError, TypeError, ValidationError) as error:
            detail = error.message if isinstance(error, ValidationError) else str(error)
            raise InvalidModelAction(f"{name}: {detail}") from error


def conversation_messages(message, *, redundant_tool_text=False):
    """Preserve assistant output in Cohere's accepted tool-history representation.

    North rejects legacy tool_plan and text blocks alongside tool_calls. Preserve
    planning as modern thinking content, and split public text from tool calls
    into adjacent assistant messages. Both forms are accepted by the live API.
    Exact unmodified provider responses are retained separately for auditing.
    """
    data = message.model_dump(exclude_none=True) if not isinstance(message, dict) else copy.deepcopy(message)
    plan = data.pop("tool_plan", None)
    content = data.pop("content", [])
    thoughts = ([plan] if plan else []) + [block["thinking"] for block in content if block.get("type") == "thinking"]
    texts = [block for block in content if block.get("type") == "text"]
    if redundant_tool_text and data.get("tool_calls"):
        texts = []  # The validated call already carries every argument; original text is archived.
    content = ([{"type": "thinking", "thinking": "\n".join(thoughts)}] if thoughts else []) + texts
    if data.get("tool_calls") and texts:
        return [{"role": "assistant", "content": content}, data]
    if content:
        data["content"] = content
    return [data] if len(data) > 1 else []


def normalize_text_tool_calls(response, tools):
    """Bridge complete model-authored JSON tool requests, never prose or partial arguments.

    Some North responses put {tool_name, parameters} requests in text rather than
    the native tool-call field. Preserve that text and attach validated, uniquely
    identified calls so tool results have a balanced native conversation history.
    """
    if response.message.tool_calls:
        return response, "native", None
    text = "".join(block.text for block in response.message.content or [] if block.type == "text").strip()
    if text.startswith("```json\n") and text.endswith("```"):
        text = text[len("```json\n"):-3].strip()
    elif text.startswith("```\n") and text.endswith("```"):
        text = text[4:-3].strip()
    if not text or text[0] not in "[{":
        return response, "none", None
    try:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Observed transport quirk: complete request objects followed by a
            # closing list bracket, with only the opening bracket omitted.
            if text.startswith("{") and text.endswith("]"):
                data = json.loads("[" + text)
            else:
                raise
        if isinstance(data, dict) and set(data) == {"tool_calls"}:
            data = data["tool_calls"]
        calls = data if isinstance(data, list) else [data]
        if not calls:
            raise ValueError("The JSON action list is empty.")
        schemas = {tool["function"]["name"]: tool["function"]["parameters"] for tool in tools}
        normalized = []
        for item in calls:
            if not isinstance(item, dict):
                raise ValueError("Every action must be an object.")
            if {"tool_name", "parameters"} <= set(item) and set(item) <= {"tool_call_id", "tool_name", "parameters"}:
                name, arguments = item["tool_name"], item["parameters"]
            elif "function" in item and set(item) <= {"id", "type", "function"}:
                function = item["function"]
                if not isinstance(function, dict) or set(function) != {"name", "arguments"}:
                    raise ValueError("A function request must contain exactly name and arguments.")
                name, arguments = function["name"], function["arguments"]
                if isinstance(arguments, str):
                    arguments = json.loads(arguments)
            else:
                raise ValueError("Use an explicit tool_name and parameters object; examples or other JSON are not actions.")
            if name not in schemas:
                raise ValueError(f"Unknown or unavailable tool: {name}")
            Draft202012Validator(schemas[name]).validate(arguments)
            normalized.append(dict(id=f"text-tool-{uuid4().hex}", type="function",
                                   function=dict(name=name, arguments=json.dumps(arguments))))
        data = response.model_dump(exclude_none=True)
        data["message"]["tool_calls"] = normalized
        return V2ChatResponse.model_validate(data), "text_json", None
    except (ValueError, KeyError, TypeError, ValidationError) as error:
        detail = error.message if isinstance(error, ValidationError) else str(error)
        return response, "invalid_text_json", detail


class StreamAssembler:
    def __init__(self):
        self.id = None
        self.content, self.tools, self.citations = {}, {}, {}
        self.tool_plan = ""
        self.closed_tools = set()
        self.finish_reason = None
        self.usage = None
        self.error = None
        self.finished = False
        self.events = 0
        self.output_chars = 0
        self.public_text = ""
        self.phase = "waiting"

    def feed(self, event):
        data = event if isinstance(event, dict) else event.model_dump(exclude_none=True)
        kind = data["type"]
        index = data.get("index", 0)
        delta = data.get("delta") or {}
        message = delta.get("message") or {}
        self.events += 1
        if data.get("id"):
            self.id = data["id"]
        if kind in ("content-start", "content-delta"):
            fragment = message.get("content") or {}
            block = self.content.setdefault(index, {})
            for key, value in fragment.items():
                if key in ("text", "thinking"):
                    block[key] = block.get(key, "") + value
                    self.output_chars += len(value)
                    if key == "text":
                        self.public_text += value
                else:
                    block[key] = value
            block.setdefault("type", "thinking" if "thinking" in block else "text")
            self.phase = "thinking" if block["type"] == "thinking" else "text"
        elif kind == "content-end":
            self.phase = "finishing"
        elif kind == "tool-plan-delta":
            text = message.get("tool_plan") or ""
            self.tool_plan += text
            self.output_chars += len(text)
            self.phase = "thinking"
        elif kind in ("tool-call-start", "tool-call-delta"):
            self.phase = "tools"
            fragment = message.get("tool_calls") or {}
            call = self.tools.setdefault(index, {"type": "function", "function": {"arguments": ""}})
            for key, value in fragment.items():
                if key != "function":
                    call[key] = value
            for key, value in (fragment.get("function") or {}).items():
                if key == "arguments":
                    call["function"][key] = call["function"].get(key, "") + value
                    self.output_chars += len(value)
                else:
                    call["function"][key] = value
        elif kind == "tool-call-end":
            self.closed_tools.add(index)
            if set(self.tools) <= self.closed_tools:
                self.phase = "finishing"
        elif kind == "citation-start":
            self.citations[index] = copy.deepcopy(delta.get("citation") or delta)
        elif kind == "message-end":
            self.finish_reason = delta.get("finish_reason")
            self.usage = delta.get("usage")
            self.error = delta.get("error")
            self.finished = True
            self.phase = "complete"

    def progress(self):
        return dict(events=self.events, output_chars=self.output_chars, phase=self.phase,
                    tool_names=[c["function"].get("name", "") for _, c in sorted(self.tools.items())],
                    public_text=self.public_text[-300:], finished=self.finished)

    def response(self):
        if not self.finished or not self.finish_reason:
            raise IncompleteModelStream("The response stream ended before message-end; no draft tool calls were executed.")
        if self.error:
            raise IncompleteModelStream(self.error)
        if self.finish_reason != "MAX_TOKENS" and (set(self.tools)-self.closed_tools):
            raise IncompleteModelStream("The response ended with unfinished tool calls; no draft actions were executed.")
        message = {"role": "assistant"}
        if self.content:
            message["content"] = [v for _, v in sorted(self.content.items())]
        if self.tools:
            message["tool_calls"] = [v for _, v in sorted(self.tools.items())]
        if self.tool_plan:
            message["tool_plan"] = self.tool_plan
        if self.citations:
            message["citations"] = [v for _, v in sorted(self.citations.items())]
        return V2ChatResponse.model_validate(dict(id=self.id or "stream-response", message=message,
                                                  finish_reason=self.finish_reason, usage=self.usage))
