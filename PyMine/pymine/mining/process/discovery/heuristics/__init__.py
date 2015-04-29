from collections import defaultdict


class Matrix(object):

    class Cell(object):
        def __init__(self, key, value):
            self.key = key
            self.value = value

        def __str__(self):
            return '%s: %s' % (self.key, self.value)

        def __repr__(self):
            return '%s: %s' % (self.key, self.value)

    class Column(object):
        def __init__(self, values=None):
            self._column = values or defaultdict(float)

        def get_dict(self):
            return self._column

        @property
        def cells(self):
            return [Matrix.Cell(k, v) for k, v in self._column.items()]

        def __getitem__(self, item):
            return self._column[item]

        def __setitem__(self, key, value):
            self._column[key] = value

        def __str__(self):
            return str(self._column)

        def __repr__(self):
            return repr(self._column)

        def __iter__(self):
            return iter(self._column)

        def __getattr__(self, item):
            return getattr(self._column, item)

    def __init__(self):
        self._matrix = defaultdict(lambda: Matrix.Column())

    def __getitem__(self, item):
        return self._matrix[item]

    def __setitem__(self, key, value):
        return self._matrix.__setitem__(key, value)

    def __str__(self):
        return str(self._matrix)

    def __nonzero__(self):
        return bool(self._matrix)

    def __iter__(self):
        return iter(self._matrix)

    def __getattr__(self, item):
        return getattr(self._matrix, item)

    def get_column(self, item):
        cells = []
        for e in self._matrix:
            cells.append(Matrix.Cell(e, self._matrix[e][item]))
        return cells



