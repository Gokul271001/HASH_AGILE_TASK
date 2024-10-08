def find_non_repeating_char(input_string):
    count_tracker = {}
    for ch in input_string:
        if ch in count_tracker:
            count_tracker[ch] += 1
        else:
            count_tracker[ch] = 1
    for ch in input_string:
        if count_tracker[ch] == 1:
            return ch
    return "Every character repeats."
test_string = input()
print("First non-repeating character in ",find_non_repeating_char(test_string))