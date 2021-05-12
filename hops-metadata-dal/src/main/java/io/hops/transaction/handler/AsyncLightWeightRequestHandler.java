package io.hops.transaction.handler;

import java.io.IOException;

/**
 * Created by antonis on 8/29/16.
 */
public abstract class AsyncLightWeightRequestHandler extends LightWeightRequestHandler {
    public AsyncLightWeightRequestHandler(OperationType opType) {
        super(opType);
    }

    @Override
    public Object handle() throws IOException {
        ThreadPool.getInstance().getCommitThreadPool()
                .submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            AsyncLightWeightRequestHandler.super.handle();
                        } catch (IOException ex) {
                            requestHandlerLOG.error(ex, ex);
                        }
                    }
                });

        return null;
    }
}
