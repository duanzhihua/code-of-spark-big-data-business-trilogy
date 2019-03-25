package com.dt.spark.IMF114;

public class IMFJavaBean {

	public static void main(String[] args) {
		AdClicked  adClicked=new AdClicked();
		adClicked.setAdID("1234");
		adClicked.setCity("ShangHai");
		adClicked.setTimestamp("2016-7-9");
		adClicked.setCity("ShangHai");
		adClicked.setClickedCount((long) 1000);
		System.out.println(adClicked.toString());
	}

}

class AdClicked {
	private String timestamp;
	private String adID;
	private String province;
	private String city;
	private Long clickedCount;

	@Override
	public String toString() {
		return "AdClicked [timestamp=" + timestamp + ", adID=" + adID + ", province=" + province + ", city=" + city
				+ ", clickedCount=" + clickedCount + "]";
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getAdID() {
		return adID;
	}

	public void setAdID(String adID) {
		this.adID = adID;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Long getClickedCount() {
		return clickedCount;
	}

	public void setClickedCount(Long clickedCount) {
		this.clickedCount = clickedCount;
	}

}
