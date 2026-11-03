# Cormac Reynolds: Jan 2006 original version
# a bunch of fairly generic functions 

class Tee:
    # simple class that allows you to print to more than one file at once. A
    # bit like the unix command tee. Should accept either a file/file-like
    # object or a filehandle
    """arguments are the writemode for files that are not already open ('a' or
    'w' usually) and the files/filehandles to which you want the output
    printed"""

    def __init__(self, writemode, *args):
        self._files = []
        for file in args:
            if not hasattr( file, 'write' ):
                file = open( file, writemode )

            self._files.append(file)

    def write(self, string):
        for file in self._files:
            file.write(string)
        self.flush()

    def flush(self):
        for file in self._files:
            file.flush()

class Msg_Filter:
    """ writes output to a file (or similar object) after filtering. First
    argument is the desired output object, remainder are the strings to
    filter on. Should itself look like a file object. Filter is
    case-insensitive"""

    def __init__(self, file, *args):
        self._file = file
        self._filters = args

    def write(self, string):
        for filter_text in self._filters:
            if filter_text.lower() in string.lower():
                self._file.write (string)
                break
        self.flush()

    def flush(self):
        self._file.flush()
