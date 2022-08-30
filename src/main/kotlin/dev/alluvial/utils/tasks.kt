package dev.alluvial.utils

fun interface Task<I, O> {
    fun run(input: I): O
}

interface Callback<I, O> {
    fun beforeExecute(input: I) {}
    fun onSuccess(input: I, result: O) {}
    fun onFailure(input: I, throwable: Throwable) {}
}

class CallbackTask<I, O>(
    private val task: Task<I, O>
) : Task<I, O> {
    private val callbacks = mutableListOf<Callback<I, O>>()

    fun addCallback(callback: Callback<I, O>) {
        callbacks.add(callback)
    }

    override fun run(input: I): O {
        try {
            beforeExecute(input)
            val output = task.run(input)
            onSuccess(input, output)
            return output
        } catch (throwable: Throwable) {
            onFailure(input, throwable)
            throw throwable
        }
    }

    private fun beforeExecute(input: I) {
        for (callback in callbacks) {
            callback.beforeExecute(input)
        }
    }

    private fun onSuccess(input: I, output: O) {
        for (callback in callbacks) {
            callback.onSuccess(input, output)
        }
    }

    private fun onFailure(input: I, throwable: Throwable) {
        for (callback in callbacks) {
            try {
                callback.onFailure(input, throwable)
            } catch (t: Throwable) {
                throwable.addSuppressed(t)
            }
        }
    }
}
