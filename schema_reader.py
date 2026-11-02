"""Read a JSON Schema and normalize it into Object/Array/Scalar nodes."""

import argparse
import json
from typing import Any


def load_schema(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as file:
        return json.load(file)


def resolve_ref(
    root_schema: dict[str, Any],
    ref: str,
) -> dict[str, Any]:
    # 초기 버전은 #/$defs/.. 같은 로컬 참조만 지원
    if not ref.startswith("#/"):
        raise ValueError(f"지원하지 않는 스키마 참조: {ref}")

    node: Any = root_schema

    # JSON Pointer 의 경로 조각을 따라 스키마 내부로 이동
    for token in ref[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        node = node[token]

    return node


def parse_node(
    fragment: dict[str, Any],
    root_schema: dict[str, Any],
    ref_stack: tuple[str, ...] = (),
) -> dict[str, Any]:
    ref = fragment.get("$ref")

    if ref:
        if ref in ref_stack:
            raise ValueError(f"순환 참조 발견: {ref}")

        resolved = resolve_ref(root_schema, ref)
        node = parse_node(resolved, root_schema, ref_stack + (ref,))
        return {**node, "source_ref": ref}

    schema_type = fragment.get("type")

    if schema_type == "object":
        required_names = set(fragment.get("required", []))
        fields = []

        for name, child_schema in fragment.get("properties", {}).items():
            fields.append({
                "name": name,
                "required": name in required_names,
                "node": parse_node(child_schema, root_schema, ref_stack),
            })

        node = {
            "kind": "object",
            "fields": fields,
        }

        if "additionalProperties" in fragment:
            node["additional_properties"] = fragment["additionalProperties"]

        return node

    if schema_type == "array":
        if "items" not in fragment:
            raise ValueError("Array Schema에 items 정의가 없습니다.")

        return {
            "kind": "array",
            "items": parse_node(fragment["items"], root_schema, ref_stack),
        }

    if schema_type in {"string", "integer", "number", "boolean", "null"}:
        node = {
            "kind": "scalar",
            "json_type": schema_type,
        }

        # 변환에 활용할 수 있는 메타정보를 유지
        for key in ("format", "pattern", "enum", "minimum", "maximum"):
            if key in fragment:
                node[key] = fragment[key]

        return node

    raise ValueError(f"지원하지 않는 JSON Schema 타입입니다: {schema_type}")


def read_schema_tree(path: str) -> dict[str, Any]:
    schema = load_schema(path)
    return parse_node(schema, schema)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read and normalize a JSON Schema")
    parser.add_argument("--schema", required=True, help="JSON Schema file path")
    args = parser.parse_args()
    print(read_schema_tree(args.schema))
