
import wrapt


def skip_on_exception(exp):
 

    from pytest import skip

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        try:
            return wrapped(*args, **kwargs)
        except exp as e:
            skip(str(e))

    return wrapper
