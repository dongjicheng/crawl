package com.meritco;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONObject;

import de.robv.android.xposed.XposedBridge;
import de.robv.android.xposed.XposedHelpers;
import de.robv.android.xposed.callbacks.XC_LoadPackage.LoadPackageParam;

public class CommandReceiver implements Runnable {

	private URL url;
	private LoadPackageParam lpparam;

	public CommandReceiver(LoadPackageParam lpparam, URL url) {
		this.url = url;
		this.lpparam = lpparam;
	}

	@Override
	public void run() {
		XposedBridge.log("--- CommandReceiver.run ---");
		while (true) {
			HttpURLConnection urlConnection = null;
			try {
				urlConnection = (HttpURLConnection) url.openConnection();
				urlConnection.setConnectTimeout(10000);
				urlConnection.setUseCaches(false);

				InputStream in = urlConnection.getInputStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				StringBuilder response = new StringBuilder();

				for (String line = null; (line = br.readLine()) != null;) {
					response.append(line);
				}

				urlConnection.disconnect();
				urlConnection = null;
				
				JSONObject jsoncmd = new JSONObject(response.toString());
				XposedBridge.log(jsoncmd.toString());

				if( jsoncmd.isNull("ClassName") ){
					XposedBridge.log("--- Thread.sleep ---");
					Thread.sleep(10000);
					continue;
				}
				
				if (jsoncmd.getString("ClassName").equals("UserCardLiteRequest")) {
					Class<?> UserCardLite = XposedHelpers.findClass("com.immomo.molive.api.UserCardLiteRequest",
							lpparam.classLoader);
					Object handle = XposedHelpers.newInstance(UserCardLite, jsoncmd.getString("remoteid"),
							jsoncmd.getString("roomid"), "", null);
					XposedHelpers.callMethod(handle, "tailSafeRequest");
				}
				else if (jsoncmd.getString("ClassName").equals("RoomRankingTotalStarsRequest")) {
					Class<?> RoomRankingStarRequest = XposedHelpers.findClass("com.immomo.molive.api.RoomRankingTotalStarsRequest",
							lpparam.classLoader);
					Object handle = XposedHelpers.newInstance(RoomRankingStarRequest, jsoncmd.getString("roomid"),
							jsoncmd.getString("starid"), null);
					XposedHelpers.callMethod(handle, "tailSafeRequest");
				}
				else if (jsoncmd.getString("ClassName").equals("RoomProfileFullRequest")){
					Class<?> RoomProfileFullRequest = XposedHelpers.findClass("com.immomo.molive.api.RoomProfileFullRequest",
							lpparam.classLoader);
					Object handle = XposedHelpers.newInstance(RoomProfileFullRequest, "14503553030",0,"",true,true,"","");
					XposedHelpers.callMethod(handle, "tailSafeRequest");
				}

				XposedBridge.log("--- CommandReceiver complete ---");

			} catch (Exception e) {
				StringWriter sw = new StringWriter();  
	            e.printStackTrace(new PrintWriter(sw, true));
//	            XposedBridge.log(sw.toString());
	            XposedBridge.log(e.getMessage());
				try {
					if (urlConnection != null) {
						urlConnection.disconnect();
						urlConnection = null;
					}
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			} finally {
				if (urlConnection != null) {
					urlConnection.disconnect();
				}
			}
		}
	}
}
