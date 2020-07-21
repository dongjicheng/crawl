package com.meritco;

import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONObject;

import java.lang.reflect.Field;
import java.net.MalformedURLException;

import android.content.Intent;
import android.content.pm.ApplicationInfo;
import de.robv.android.xposed.XC_MethodHook;
import de.robv.android.xposed.XposedBridge;
import de.robv.android.xposed.XposedHelpers;
import de.robv.android.xposed.IXposedHookLoadPackage;
import de.robv.android.xposed.callbacks.XC_LoadPackage.LoadPackageParam;

public class XposedModule implements IXposedHookLoadPackage {

	private boolean HAS_REGISTER_LISENER = false;

	@Override
	public void handleLoadPackage(final LoadPackageParam lpparam) throws Throwable {

		if (lpparam.appInfo == null || (lpparam.appInfo.flags
				& (ApplicationInfo.FLAG_SYSTEM | ApplicationInfo.FLAG_UPDATED_SYSTEM_APP)) != 0) {
			return;
		} else if (!lpparam.isFirstApplication || !"com.immomo.momo".equals(lpparam.processName)) {
			return;
		}

		XposedBridge.log("--- " + lpparam.processName + " ---");

		XposedHelpers.findAndHookMethod("com.tencent.tinker.loader.app.TinkerApplication", lpparam.classLoader,
				"onCreate", new XC_MethodHook() {
					@Override
					protected void beforeHookedMethod(MethodHookParam param) throws Throwable {
						XposedBridge.log("--- TinkerApplication.onCreate beforeHookedMethod ---");
						// if (!HAS_REGISTER_LISENER) {
						// HAS_REGISTER_LISENER = true;
						// new Thread(new CommandReceiver(lpparam, new
						// URL("http://192.168.2.150:8080"))).start();
						// }
					}

					@Override
					protected void afterHookedMethod(MethodHookParam param) throws Throwable {
						XposedBridge.log("--- TinkerApplication.onCreate afterHookedMethod ---");

						Class<?> b = XposedHelpers.findClass("com.immomo.molive.api.a.b", lpparam.classLoader);
						XposedHelpers.findAndHookMethod("com.immomo.molive.api.BaseApiRequeset", lpparam.classLoader,
								"onResponse", String.class, b, new XC_MethodHook() {
									@Override
									protected void beforeHookedMethod(MethodHookParam param) throws Throwable {
										XposedBridge.log("--- BaseApiRequeset onResponse ---");

										JSONObject request = new JSONObject();
										Field mUrl = param.thisObject.getClass().getSuperclass()
												.getDeclaredField("mUrl");
										mUrl.setAccessible(true);
										request.put("url", mUrl.get(param.thisObject));

										Field mParams = param.thisObject.getClass().getSuperclass()
												.getDeclaredField("mParams");
										mParams.setAccessible(true);
										@SuppressWarnings("unchecked")
										Map<String, Object> params = (Map<String, Object>) mParams
												.get(param.thisObject);
										Iterator<Entry<String, Object>> iterator = params.entrySet().iterator();
										while (iterator.hasNext()) {
											Entry<String, Object> entry = iterator.next();
											request.put(entry.getKey(), entry.getValue());
										}
										XposedBridge.log(request.toString());

										JSONObject response = new JSONObject((String) param.args[0]);
										JSONObject data = new JSONObject();
										data.put("request", request);
										data.put("response", response);
										new Thread(new DataSender(
												new URL("http://192.168.2.150:8080/BaseApiResponse.json"),
												data.toString())).start();
									}
								});
					}
				});

	}
}
