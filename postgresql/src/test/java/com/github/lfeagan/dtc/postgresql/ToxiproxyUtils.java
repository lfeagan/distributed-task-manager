package com.github.lfeagan.dtc.postgresql;

import eu.rekawek.toxiproxy.HttpClient;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

public class ToxiproxyUtils {

    public static ToxiproxyClient reflectivelyGetToxiproxyClient(ToxiproxyContainer toxiproxyContainer) {
        try {
            Field clientField = toxiproxyContainer.getClass().getDeclaredField("client");
            clientField.setAccessible(true);
            Object o = clientField.get(toxiproxyContainer);
            return (ToxiproxyClient) o;
        } catch (Exception e) {
            throw new RuntimeException("Unable to extract ToxiproxyClient", e);
        }
    }

    public static HttpClient reflectivelyGetHttpClientFromToxiproxy(ToxiproxyClient toxiproxyClient) {
        try {
            Field httpClientField = toxiproxyClient.getClass().getDeclaredField("httpClient");
            httpClientField.setAccessible(true);
            Object o = httpClientField.get(toxiproxyClient);
            return (HttpClient) o;
        } catch (Exception e) {
            throw new RuntimeException("Unable to extract HttpClient", e);
        }
    }

    public static void deleteAllProxies(ToxiproxyContainer toxiproxyContainer) throws IOException {
        ToxiproxyClient toxiproxyClient = reflectivelyGetToxiproxyClient(toxiproxyContainer);
        HttpClient httpClient = reflectivelyGetHttpClientFromToxiproxy(toxiproxyClient);
        List<Proxy> proxies = toxiproxyClient.getProxies();
        for (Proxy proxy : proxies) {
            proxy.delete();
        }
    }
}
