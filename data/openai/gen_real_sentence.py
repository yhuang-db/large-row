import random

lines = open("webtext.test.txt").readlines()


def gen_sentence_of_length(lines, target_length):
    """Generate sentences of target length"""
    line_index = random.randint(0, len(lines))
    result = ""
    while len(result) < target_length:
        result += lines[line_index]
        line_index += 1
        if line_index >= len(lines):
            line_index = 0
    result = result[:target_length]
    return result


def gen_sentence_kb(kb, lines):
    """Generate a sentence of kb kilobytes"""
    target_length = kb * 1024
    return gen_sentence_of_length(lines, target_length)


def gen_sentence_mb(mb, lines):
    """Generate a sentence of mb megabytes"""
    target_length = mb * 1024 * 1024
    return gen_sentence_of_length(lines, target_length)
