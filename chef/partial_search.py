import six
import collections
import copy
import six.moves.urllib.parse
import json
from chef.api import ChefAPI
from chef.base import ChefQuery, ChefObject

class PartialSearch(collections.Sequence):
    """A partial search of the Chef index.
    
    The only required argument is the index name to search (eg. node, role, etc).
    The second, optional argument can be any Solr search query, with the same semantics
    as Chef.
    
    Example::
    
        for row in PartialSearch('node', 'roles:app', keys={'hostname': ['hostname'], 'roles': ['roles'] }):
            print row['roles']
    
    .. versionadded:: 0.1
    """

    url = '/search'

    def __init__(self, index, q='*:*', rows=1000, start=0, api=None, keys={}):
        self.name = index
        self.api = api or ChefAPI.get_global()
        self._args = dict(q=q, rows=rows, start=start)
        self.keys = keys
        self.url = self.__class__.url + '/' + self.name + '?' + six.moves.urllib.parse.urlencode(self._args)
      

    @property
    def data(self):
        if not hasattr(self, '_data'):
            self._data = self.api.api_request('POST', self.url, {}, self.keys)
        return self._data

    @property
    def total(self):
        return self.data['total']

    def query(self, query):
        args = copy.copy(self._args)
        args['q'] = query
        return self.__class__(self.name, api=self.api, **args)

    def rows(self, rows):
        args = copy.copy(self._args)
        args['rows'] = rows
        return self.__class__(self.name, api=self.api, **args)

    def start(self, start):
        args = copy.copy(self._args)
        args['start'] = start
        return self.__class__(self.name, api=self.api, **args)

    def __len__(self):
        return len(self.data['rows'])

    def __getitem__(self, value):
        if isinstance(value, slice):
            if value.step is not None and value.step != 1:
                raise ValueError('Cannot use a step other than 1')
            return self.start(self._args['start']+value.start).rows(value.stop-value.start)
        if isinstance(value, six.string_types):
            return self[self.index(value)]
        row_value = self.data['rows'][value]
        # Check for null rows, just in case
        if row_value is None:
            return None
        return row_value['data']

    def __contains__(self, name):
        for row in self:
            if row.object.name == name:
                return True
        return False

    def index(self, name):
        for i, row in enumerate(self):
            if row.object.name == name:
                return i
        raise ValueError('%s not in search'%name)

    def __call__(self, query):
        return self.query(query)

    @classmethod
    def list(cls, api=None):
        api = api or ChefAPI.get_global()
        names = [name for name, url in six.iteritems(api[cls.url])]
        return ChefQuery(cls, names, api, data=self.keys)
