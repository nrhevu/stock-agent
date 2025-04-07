import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from nlp.translate import parse_date, translate_text


def test_translate():
    translate = translate_text("bài thơ em viết, trao anh ngày ấy")
    print(translate)
    return translate

test_translate()