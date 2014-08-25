# -*- coding: utf-8 -*-
import unittest
import zlib
import pickle
import memcache


class FakeSocket(object):
    def __init__(self):
        self.responses = []
        self.commands = []

    def __call__(self, *args, **kwds):
        return self

    def clear(self):
        self.commands = []
        self.responses = []

    def recv(self, *args, **kwds):
        if self.responses:
            return self.responses.pop(0) + '\r\n'
        else:
            return ''

    def sendall(self, longcmd):
        cmds = longcmd.split('\r\n')
        if cmds[-1] == '':
            cmds = cmds[:-1]
        for cmd in cmds:
            self.commands.append(cmd)

    def noop(self, *args, **kwds):
        pass

    connect = noop
    close = noop
    setsockopt = noop


class TestMemcacheClient(unittest.TestCase):
    def setUp(self):
        self.client = memcache.Client(
            ['127.0.0.1:000'],
            pickleProtocol=2,
            cache_cas=True)
        self.sock = FakeSocket()
        self._oldsocket = memcache.socket.socket
        memcache.socket.socket = self.sock
        self.sock.clear()

    def tearDown(self):
        memcache.socket.socket = self._oldsocket

    def test_get(self):
        self.sock.responses = ['VALUE test-int 2 1', '1', 'END']
        result = self.client.get('test-int')
        self.assertEqual(self.sock.commands, ['get test-int'])
        self.assertEqual(result, 1)

        # test no value
        self.sock.clear()
        self.sock.responses = ['END']
        result = self.client.get('test-int')
        self.assertEqual(self.sock.commands, ['get test-int'])
        self.assertEqual(result, None)

    def test_incr(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.incr('test')
        self.assertEqual(self.sock.commands, ['incr test 1'])
        self.assertEqual(response, None)

        self.sock.clear()
        self.sock.responses = ['2']
        response = self.client.incr('test', noreply=False)
        self.assertEqual(self.sock.commands, ['incr test 1'])
        self.assertEqual(response, 2)

        self.sock.clear()
        self.sock.responses = []
        response = self.client.incr('test', noreply=True)
        self.assertEqual(self.sock.commands, ['incr test 1 noreply'])
        self.assertEqual(response, None)

    def test_decr(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.decr('test')
        self.assertEqual(self.sock.commands, ['decr test 1'])
        self.assertEqual(response, None)

        self.sock.clear()
        self.sock.responses = ['2']
        response = self.client.decr('test', noreply=False)
        self.assertEqual(self.sock.commands, ['decr test 1'])
        self.assertEqual(response, 2)

        self.sock.clear()
        self.sock.responses = []
        response = self.client.decr('test', noreply=True)
        self.assertEqual(self.sock.commands, ['decr test 1 noreply'])
        self.assertEqual(response, None)

    def test_add(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.add('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['add test 2 0 1', '1'])
        self.assertEqual(response, False)

        self.sock.clear()
        self.sock.responses = ['STORED']
        response = self.client.add('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['add test 2 0 1', '1'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = []
        response = self.client.add('test', 1, noreply=True)
        self.assertEqual(self.sock.commands, ['add test 2 0 1 noreply', '1'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = ['NOT_STORED']
        response = self.client.add('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['add test 2 0 1', '1'])
        self.assertEqual(response, False)

    def test_replace(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.replace('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['replace test 2 0 1', '1'])
        self.assertEqual(response, False)

        self.sock.clear()
        self.sock.responses = ['STORED']
        response = self.client.replace('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['replace test 2 0 1', '1'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = []
        response = self.client.replace('test', 1, noreply=True)
        self.assertEqual(self.sock.commands, ['replace test 2 0 1 noreply', '1'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = ['NOT_STORED']
        response = self.client.replace('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['replace test 2 0 1', '1'])
        self.assertEqual(response, False)

    def test_prepend(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.prepend('test', "prefix", noreply=False)
        self.assertEqual(self.sock.commands, ['prepend test 0 0 6', 'prefix'])
        self.assertEqual(response, False)

        self.sock.clear()
        self.sock.responses = ['STORED']
        response = self.client.prepend('test', "prefix", noreply=False)
        self.assertEqual(self.sock.commands, ['prepend test 0 0 6', 'prefix'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = []
        response = self.client.prepend('test', "prefix", noreply=True)
        self.assertEqual(self.sock.commands, ['prepend test 0 0 6 noreply', 'prefix'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = ['NOT_STORED']
        response = self.client.prepend('test', "prefix", noreply=False)
        self.assertEqual(self.sock.commands, ['prepend test 0 0 6', 'prefix'])
        self.assertEqual(response, False)

    def test_append(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.append('test', "postfix", noreply=False)
        self.assertEqual(self.sock.commands, ['append test 0 0 7', 'postfix'])
        self.assertEqual(response, False)

        self.sock.clear()
        self.sock.responses = ['STORED']
        response = self.client.append('test', "postfix", noreply=False)
        self.assertEqual(self.sock.commands, ['append test 0 0 7', 'postfix'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = []
        response = self.client.append('test', "postfix", noreply=True)
        self.assertEqual(self.sock.commands, ['append test 0 0 7 noreply', 'postfix'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = ['NOT_STORED']
        response = self.client.append('test', "postfix", noreply=False)
        self.assertEqual(self.sock.commands, ['append test 0 0 7', 'postfix'])
        self.assertEqual(response, False)

    def test_cas(self):
        self.client.cache_cas = True
        self.sock.responses = ['NOT_FOUND']
        response = self.client.gets('test')
        self.assertEqual(self.sock.commands, ['gets test'])
        self.assertEqual(response, None)

        self.sock.clear()
        self.sock.responses = ['VALUE test 0 7 123', 'abcdefg', 'END']
        response = self.client.gets('test')
        self.assertEqual(self.sock.commands, ['gets test'])
        self.assertEqual(response, 'abcdefg')
        self.assertEqual(self.client.cas_ids['test'], 123)
        self.sock.clear()
        self.sock.responses = ['STORED']
        response = self.client.cas('test', 'gfedcba')
        self.assertEqual(self.sock.commands, ['cas test 0 0 7 123', 'gfedcba'])
        self.assertEqual(response, True)
        self.sock.clear()
        self.sock.responses = ['EXISTS']
        response = self.client.cas('test', 'gfedcba')
        self.assertEqual(self.sock.commands, ['cas test 0 0 7 123', 'gfedcba'])
        self.assertEqual(response, False)

    def test_delete(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.delete('test', noreply=False)
        self.assertEqual(self.sock.commands, ['delete test'])
        self.assertEqual(response, 0)

        self.sock.clear()
        self.sock.responses = ['DELETED']
        response = self.client.delete('test', noreply=False)
        self.assertEqual(self.sock.commands, ['delete test'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = ['']
        response = self.client.delete('test', noreply=True)
        self.assertEqual(self.sock.commands, ['delete test noreply'])
        self.assertEqual(response, 1)

    def test_touch(self):
        self.sock.responses = ['NOT_FOUND']
        response = self.client.touch('test', noreply=False)
        self.assertEqual(self.sock.commands, ['touch test'])
        self.assertEqual(response, 0)

        self.sock.clear()
        self.sock.responses = ['TOUCHED']
        response = self.client.touch('test', noreply=False)
        self.assertEqual(self.sock.commands, ['touch test'])
        self.assertEqual(response, True)

        self.sock.clear()
        self.sock.responses = ['']
        response = self.client.touch('test', noreply=True)
        self.assertEqual(self.sock.commands, ['touch test noreply'])
        self.assertEqual(response, 1)

    def test_set(self):
        self.sock.responses = ['STORED']
        self.client.set('test-int', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['set test-int 2 0 1', '1'])

        self.sock.clear()
        self.client.set('test-int', 1, noreply=True)
        self.assertEqual(self.sock.commands,
                         ['set test-int 2 0 1 noreply', '1'])

    def test_set_flags(self):
        ## string
        self.sock.responses = ['STORED']
        self.client.set('test', 'test!', noreply=False)
        self.assertEqual(self.sock.commands, ['set test 0 0 5', 'test!'])

        ## integer
        self.sock.clear()
        self.sock.responses = ['STORED']
        self.client.set('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['set test 2 0 1', '1'])

        ## integer
        self.sock.clear()
        self.sock.responses = ['STORED']
        self.client.set('test', 1, noreply=False)
        self.assertEqual(self.sock.commands, ['set test 2 0 1', '1'])

        ## long
        self.sock.clear()
        self.sock.responses = ['STORED']
        self.client.set('test', 1L, noreply=False)
        self.assertEqual(self.sock.commands, ['set test 4 0 1', '1'])

        ## compress
        self.sock.clear()
        self.sock.responses = ['STORED']
        long_string = 'abc' * 255
        self.client.set('test', long_string, min_compress_len=1, noreply=False)
        cp = zlib.compress(long_string)
        self.assertEqual(len(self.sock.commands), 2)
        self.assertEqual(self.sock.commands[0], 'set test 8 0 %s' % len(cp))
        self.assertEqual(self.sock.commands[1], cp)

        ## pickled
        self.sock.clear()
        self.sock.responses = ['STORED']
        x = set(['1', '2', '3'])
        p = pickle.dumps(x, 2)
        self.client.set('test', x, noreply=False)
        self.assertEqual(len(self.sock.commands), 2)
        self.assertEqual(self.sock.commands[0], 'set test 1 0 %d' % len(p))
        self.assertEqual(self.sock.commands[1], p)
