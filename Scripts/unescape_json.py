
#!/usr/bin/env python3
"""
Simple script to unescape JSON strings.
Paste a JSON-escaped string and get the clean output.
"""

import json
import sys

def unescape_json_string(text):
    """Unescape a JSON string."""
    try:
        # Try to parse as JSON string (if it's wrapped in quotes)
        if text.strip().startswith('"') and text.strip().endswith('"'):
            return json.loads(text)
        else:
            # If not wrapped in quotes, add them and parse
            return json.loads(f'"{text}"')
    except json.JSONDecodeError:
        # If JSON parsing fails, just return the original
        return text

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # If arguments provided, process them
        input_text = " ".join(sys.argv[1:])
        print(unescape_json_string(input_text))
    else:
        # Interactive mode
        print("Paste your JSON-escaped string (press Enter twice when done):")
        print("-" * 60)
        lines = []
        empty_count = 0
        
        while empty_count < 1:
            try:
                line = input()
                if line.strip() == "":
                    empty_count += 1
                else:
                    empty_count = 0
                    lines.append(line)
            except EOFError:
                break
        
        if lines:
            input_text = "\n".join(lines)
            print("\n" + "=" * 60)
            print("UNESCAPED OUTPUT:")
            print("=" * 60)
            print(unescape_json_string(input_text))
