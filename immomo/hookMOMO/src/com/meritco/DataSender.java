package com.meritco;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import de.robv.android.xposed.XposedBridge;

public class DataSender implements Runnable {

	private URL url;
	private String data;

	public DataSender(URL url, String data) {
		this.url = url;
		this.data = data;
	}

	@Override
	public void run() {
		XposedBridge.log(String.format("--- DataSender length %d ---", data.length()));
		HttpURLConnection urlConnection = null;
		try {
			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestMethod("POST");
			urlConnection.setConnectTimeout(3000);
			urlConnection.setUseCaches(false);
			urlConnection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
			urlConnection.setRequestProperty("Accept-Encoding", "identity");
			urlConnection.connect();
			OutputStream out = new BufferedOutputStream(urlConnection.getOutputStream());
			out.write(data.getBytes());
			out.flush();

			urlConnection.getResponseCode();
			XposedBridge.log("--- DataSender complete ---");

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw, true));
//			XposedBridge.log(sw.toString());
			XposedBridge.log(e.getMessage());
		} finally {
			if (urlConnection != null) {
				urlConnection.disconnect();
			}
		}
	}
}
