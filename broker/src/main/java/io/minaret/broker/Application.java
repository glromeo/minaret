package io.minaret.broker;

import io.micronaut.runtime.Micronaut;

/**
 * Entry point.
 *
 * Run from the IDE with the working directory set to broker/. application.yml's relative paths
 * (wwwroot, the IDL store, the certificate) resolve against that directory.
 */
public final class Application {

    private Application() {
    }

    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }

}
