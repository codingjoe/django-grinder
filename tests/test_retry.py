import datetime

from threadmill import retry


class TestExponentialBackoff:
    def test_call__exponential_backoff(self):
        """Test the ExponentialBackoff retry strategy."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(seconds=10),
            factor=2.0,
            max_retries=5,
            expected_exceptions=(ValueError,),
        )

        class DummyContext:
            def __init__(self, attempt, exception_class):
                self.attempt = attempt
                self.task_result = type(
                    "TaskResult",
                    (),
                    {
                        "errors": [
                            type("Error", (), {"exception_class": exception_class})()
                        ]
                    },
                )()

        # Test that it returns the correct delay for each attempt
        for attempt in range(5):
            context = DummyContext(attempt, ValueError)
            delay = backoff(context)
            expected_delay = min(
                backoff.base_delay * (backoff.factor**attempt), backoff.max_delay
            )
            assert delay == expected_delay

        # Test that it returns None after max_retries
        context = DummyContext(5, ValueError)
        assert backoff(context) is None

        # Test that it returns None for unexpected exceptions
        context = DummyContext(0, KeyError)
        assert backoff(context) is None

    def test_deconstruct(self):
        """Test the deconstruct method of ExponentialBackoff."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(seconds=10),
            factor=2.0,
            max_retries=5,
            expected_exceptions=(ValueError,),
        )
        class_path, args, kwargs = backoff.deconstruct()
        assert (
            class_path
            == f"{backoff.__class__.__module__}.{backoff.__class__.__qualname__}"
        )
        assert args == ()
        assert kwargs == {
            "base_delay": datetime.timedelta(seconds=1),
            "max_delay": datetime.timedelta(seconds=10),
            "factor": 2.0,
            "max_retries": 5,
            "expected_exceptions": (ValueError,),
        }

    def test_deconstruct__reconstruct(self):
        """Test that deconstruct and reconstruct work together."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(seconds=10),
            factor=2.0,
            max_retries=5,
            expected_exceptions=(ValueError,),
        )
        class_path, args, kwargs = backoff.deconstruct()
        reconstructed_backoff = retry.ExponentialBackoff(**kwargs)
        assert backoff == reconstructed_backoff
