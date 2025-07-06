from datetime import datetime, timedelta


class Singleton(type):
    """A metaclass that implements the singleton pattern.

    This metaclass ensures that only one(*) instance of a class is created and that the instance is reused for all subsequent calls.
    
    The instance is refreshed every `MAX_INSTANCE_TTL` seconds.
    
    Usage:
    ```python
    >>> class MyClass(metaclass=Singleton):
    ...     pass
        
    >>> a = MyClass()
    >>> b = MyClass()
    >>> a is b
        True
    >>> sleep(MyClass.MAX_INSTANCE_TTL + 1)
    >>> c = MyClass()
    >>> a is c
        False
    ```
    """

    _instances = {}
    _creation_time = {}
    
    MAX_INSTANCE_TTL = timedelta(seconds=5 * 60)  # 5 minutes

    def __create_instance(cls, *args, **kwargs):
        """Create a new instance of this class."""

        cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        cls._creation_time[cls] = datetime.now()

    def __call__(cls, *args, force_recreate=False, **kwargs):
        """Create a new instance of this class if it does not exist or if it is no longer valid.
        
        If the `force_recreate` parameter is set to `True`, a new instance will be created regardless of the validity of the existing instance.
        """

        # Create a new instance if it does not exist or if `force_recreate` is set.
        if force_recreate or (cls not in cls._instances):
            cls.__create_instance(*args, **kwargs)
        # Create a new instance if the existing instance is no longer valid.
        elif (cls not in cls._creation_time) or (cls._creation_time[cls] + cls.MAX_INSTANCE_TTL < datetime.now()):
            cls.__create_instance(*args, **kwargs)

        # Return the existing instance.
        return cls._instances[cls]
