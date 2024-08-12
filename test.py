from collections import Counter


# String = ‘the quick brown fox jumps over the lazy dog.’

# Find the frequency of each letter in the given string
string = 'the quick brown fox jumps over the lazy dog.'
string = string.replace(' ', '').lower()
frequency = Counter(string)
print(frequency)
for letter, count in frequency.items():
    print(f"'{letter}': {count}")

    from collections import defaultdict

string = 'the quick brown fox jumps over the lazy dog.'

# Removing spaces and converting to lowercase
string = string.replace(' ', '').lower()

# Initialize a defaultdict with integer default values
frequency = defaultdict(int)

# Loop through each character in the string
for char in string:
    # Skip non-alphabetic characters
    if char.isalpha():
        # Increment the count for each character
        frequency[char] += 1

# Printing the frequency of each letter
for letter, count in frequency.items():
    print(f"'{letter}': {count}")
