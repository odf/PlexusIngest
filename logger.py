import sys

LOGGER_TRACE   = 0
LOGGER_INFO    = 1
LOGGER_WARNING = 2
LOGGER_ERROR   = 3
LOGGER_PANIC   = 4

PRIORITY_NAME = [ "TRACE",
                  "INFO",
                  "WARNING",
                  "ERROR",
                  "PANIC"  ]


class Logger:
    """
    A global logger class.
    
    Uses the Borg design pattern to share state between all instances.
    (See http://code.activestate.com/recipes/66531)
    """
    
    __shared_state = {}
    
    def __init__(self):
        self.__dict__ = self.__shared_state
        if len(self.__dict__) == 0:
            self.prefix = self.default_prefix
            self.stream = sys.stderr
            self.priority = LOGGER_WARNING
            self._stack = []

    @property
    def tag(self):
        return (self._stack or (None,))[-1]

    @property
    def level(self):
        return len(self._stack)

    def enter(self, tag = None):
        self._stack.append(tag)

    def leave(self, tag = None):
        tag = tag or self.tag
        while self.level > 0 and self._stack.pop() != tag:
            pass

    def default_prefix(self, priority, tag):
        if tag:
            tag = " %s:" % tag
        return "#(%s)%s " % (PRIORITY_NAME[priority], tag or "")

    def write(self, text, priority = LOGGER_INFO):
        if priority >= self.priority:
            pre = "  " * self.level + self.prefix(priority, self.tag)
            self.stream.write("\n".join(pre + s for s in text.split("\n") if s))
            if text.endswith("\n"):
                self.stream.write("\n")
            self.stream.flush()
        
    def writeln(self, text, priority = LOGGER_INFO):
        self.write(text + "\n", priority = priority)

    def trace(self, text):
        self.writeln(text, LOGGER_TRACE)

    def info(self, text):
        self.writeln(text, LOGGER_INFO)

    def warn(self, text):
        self.writeln(text, LOGGER_WARNING)

    def error(self, text):
        self.writeln(text, LOGGER_ERROR)

    def panic(self, text):
        self.writeln(text, LOGGER_PANIC)
