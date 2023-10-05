Current state of things:

I updated the test suite to include schema evolution stuff to properly test this.

# However there's a problem:
file_row_number doesn't work with lauren's pr. Probably first thing would be to investigate
how to make this better and allow generated columns to bypass this and only apply it to the base
columns. This should fix the current breakage with the deletes

