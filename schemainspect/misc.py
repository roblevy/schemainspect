from pkg_resources import resource_stream as pkg_resource_stream
import inspect
import six


def connection_from_s_or_c(s_or_c):  # pragma: no cover
    try:
        s_or_c.engine
        return s_or_c
    except AttributeError:
        return s_or_c.connection()


@six.python_2_unicode_compatible
class AutoRepr(object):  # pragma: no cover
    def __repr__(self):
        cname = self.__class__.__name__
        indent = ' ' * (len(cname) + 1)
        vals = [
            '{}={}'.format(k, repr(v))
            for k, v in sorted(self.__dict__.items()) if not k.startswith('_')
        ]

        return '\n{}({})\n'.format(self.__class__.__name__,
                                   ',\n{}'.format(indent).join(vals))

    def __str__(self):
        return repr(self)


def quoted_identifier(identifier):
    return '"{}"'.format(identifier.replace('"', '""'))


def external_caller():
    i = inspect.stack()
    names = (inspect.getmodule(i[x][0]).__name__ for x in range(len(i)))
    return next(name for name in names if name != __name__)


def resource_stream(subpath):
    module_name = external_caller()
    return pkg_resource_stream(module_name, subpath)


def resource_text(subpath):
    return resource_stream(subpath).read().decode('utf-8')
