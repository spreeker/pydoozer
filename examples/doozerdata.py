"""
created by stephan preeker. 2011-10

Behaves like a dict and stores data in doozerd

Doozerd is a distributes storage
which implements paxos alogorithm.

"""

import doozer
import gevent

from doozer.client import RevMismatch, TooLate, NoEntity, BadPath
from gevent import Timeout
from gevent.event import Event


class DoozerData():
    """
    class which stores data to doozerd backend

    locally we store the path -> revision numbers dict

    -all values need to be strings.
    -we watch changes to values.
        in case of a update we call the provided callback method with value.

    on initialization a path/folder can be specified where all keys
    will be stored.
    """

    def __init__(self, client, callback=None, path='/pydooz'):
        self.client = client
        self.path = path
        self.revisions = {}
        # method called when external source changes value.
        self.callback = callback
        self.watch_event = Event()
        self.watch_event.set()

        self.load_initial_data()
        # start watching for changes.
        if self.callback:
            self.watch()
            
    def load_initial_data(self):
        """
        load existing values.
        """
        walk = self.client.walk('%s/**' % self.path)
        for file in walk:
            change = self.client.get(file.path)
            self._handle_change(change)
            self.revisions[self.key_path(file.path)] = file.rev

    def watch(self):
        """
        watch the directory path for changes.
        call callback on change.

        do NOT call callback if it is a change of our own,
        thus when the revision is the same as the rev we have
        stored in out revisions.
        """

        def get_rev():
            try:
                return self.client.rev().rev
            except Timeout:
                print 'reconnecting..doozer watch'
                gevent.sleep(5)
                return get_rev()

        rev = get_rev()

        def watchjob(rev):
            change = None

            while True:

                try:
                    change = self.client.wait("%s/**" % self.path, rev)
                    # this allows to first handle a set.
                    self.watch_event.wait()
                    # print 'IN:', change.rev, change.tag, change.path
                except Timeout:
                    change = None
                    rev = get_rev()    
                    # we lost connection.. we should reload all the data.
                    self.load_initial_data()

                if change:
                    self._handle_change(change)
                    rev = change.rev + 1

        self.watchjob = gevent.spawn(watchjob, rev)

    def doozer_key(key, reverse=False):
        """
        make key appropiate for doozer
        """
        if reverse:
            return key.replace('-', '_')
        return key.replace('_', '-')

    def _handle_change(self, change):
        """
        A change has been watched. deal with it.
        """
        # get the last part of the path.
        key_path = self.key_path(change.path)
        if self._old_or_delete(key_path, change):
            return

        # print 'got change %s' % change
        self.revisions[key_path] = change.rev
        # create or update route.
        if change.flags == 4:
            self.revisions[key_path] = change.rev
            self.callback(change.value)
            return

        # down here should only be a changes with flag = 8.
        # which are your own deletes.

    def _old_or_delete(self, key_path, change):
        """
        Decide if change is our own change.

        We have to call the call_back function IF:
        -Change is NOT done by ourselves.
        -Change is a delete not from outselves.
        """
        if key_path in self.revisions:
            # check if we already have this change.
            if self.revisions[key_path] == change.rev:
                #print self.revisions[key_path] , change.rev,
                #"WE KNOW THIS ALREADY! REV DID NOT CHANGE."
                return True

            # check if it is an delete action.
            # if key_path is still in revisions it is not our delete
            # or an old delete action.
            if change.flags == 8:
                self.revisions.pop(key_path)
                self.callback(change.value, path=key_path, destroy=True)
                return True

        return False

    def get(self, key_path):
        """
        Get the latest data for path.

        get the local revision number
        if revision revnumber does not match??
           -SUCCEED and update rev number.
        return the doozer data
        """
        rev = 0
        if key_path in self.revisions:
            rev = self.revisions[key_path]
        try:
            return self.client.get(self.folder(key_path), rev)
        except RevMismatch:
            print 'revision mismach..'
            item = self.client.get(self.folder(key_path))
            self.revisions[key_path] = item.rev
            return item.value
        except TooLate:
            #XXX
            return self.client.get(self.folder(key_path))

    def set(self, key_path, value):
        """
        set a value, ONLY if you have the latest revision.
        """
        if not isinstance(value, str):
            raise TypeError('Keywords must be strings. Got: %s' % type(value))

        rev = 0
        if key_path in self.revisions:
            rev = self.revisions[key_path]
        self._set(key_path, value, rev)

    def _set(self, key_path, value, rev):
        try:
            self.watch_event.clear()
            response = self.client.set(self.folder(key_path), value, rev)
            # print 'OUT:', response.rev, response.tag, response.path
            self.watch_event.set()
            self.revisions[key_path] = response.rev
        except RevMismatch:
            print '-' * 60
            print 'ERROR failed to set "%s" "%s"' % (key_path, rev)
            print 'in doozer is a newer version of key'
            print 'loading stored route'
            print '-' * 60
            stored_change = self.client.get(self.folder(key_path))
            self._handle_change(stored_change)

    def key_path(self, path):
        return path.split('/')[-1]

    def folder(self, key_path):
        # prevent bad path in doozer. '_' is invalid.
        key_path = key_path.replace('_', '-')
        return "%s/%s" % (self.path, key_path)

    def delete(self, key_path):
        """
        Delete path. only with correct latest revision.
        """
        try:
            rev = self.revisions.get(key_path, 0)
            if rev:
                self.revisions.pop(key_path)
            item = self.client.delete(self.folder(key_path), rev)
        except RevMismatch:
            print 'ERROR! path %s rev %d RevMismatch' % (key_path, rev)
            # FIXME now we just delete.
            rev = self.client.rev().rev
            self.client.delete(self.folder(key_path), rev)

        except BadPath:
            print 'ERROR: path is bad.', self.folder(key_path)

    def delete_all(self):
        """
        clear all data.
        """
        for path, rev, value in self.items():
            try:
                item = self.client.delete(self.folder(path), rev)
            except RevMismatch:
                item = self.client.delete(self.folder(path))
                print 'value changed meanwhile', item.path, item.value
            except TooLate:
                print 'too late..'
                rev = self.client.rev().rev
                item = self.client.delete(self.folder(path), rev)

    def items(self):
        """
        return all current items from doozer.
        update local rev numbers.
        """
        try:
            folder = self.client.getdir(self.path)
        except NoEntity:
            print 'No data loaded.'
            folder = []

        for thing in folder:
            item = self.client.get(self.folder(thing.path))
            yield (thing.path, item.rev, item.value)


def print_change(change, path=None, destroy=True):
    print 'watched a change..',
    print  change, destroy, path


def change_value(d):
    gevent.sleep(1)
    d.set('test', '0')
    gevent.sleep(1)
    d.set('test2', '0')
    gevent.sleep(1)
    d.set('test', '1')


def test_doozerdata():
    client = doozer.connect()
    d = DoozerData(client, callback=print_change)
    d.set('foo1', 'bar1')
    d.set('foo2', 'bar2')
    d.set('foo3', 'bar3')
    # create a second client

    client2 = doozer.connect()
    d2 = DoozerData(client2, callback=print_change)
    d2.set('foo4', 'bar4')

    # let the second client change values to
    # those should be printed.
    cv = gevent.spawn(change_value, d2)

    for path, rev, value in d.items():
        print path, '->', value

    print d.get('foo1').value
    print d.get('foo2').value

    d.delete_all()
    # should be empty.
    print '--------'
    for di in d.items():
        print di
    assert [k for k, v in d.items()] == []
    print '--------'

    # the change value adds content over time..
    gevent.sleep(3)
    print 'data in d1'
    for di in d.items():
        print di
    print 'data in d2'
    for dii in d2.items():
        print dii

    # there is content. in both instances.
    # because the change_value job adds data later.
    cv.join(cv)
    d.delete_all()

if __name__ == '__main__':
    test_doozerdata()
