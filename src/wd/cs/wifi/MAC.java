package wd.cs.wifi;

import java.security.MessageDigest;

public class MAC {
    public MAC(){

    }

    public MAC(String mac){
        this.mac=mac.toLowerCase();
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac.toLowerCase();
    }

    String mac="00:00:c0:0b:72:e7";

    public static String calculateHashed(String mac){
        return new MAC(mac).getHashed();
    }

    public String getHashed(){
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            this.mac.replace(":", "");
            byte[] bb = md5.digest(this.mac.trim().getBytes("ASCII"));
            String stmp = "";
            StringBuilder sb = new StringBuilder("");
            for (int n = 0; n < bb.length; n++) {
                stmp = Integer.toHexString(bb[n] & 0xFF);
                sb.append((stmp.length() == 1) ? "0" + stmp : stmp);
            }
            return sb.toString().toLowerCase().trim();
        }catch(Exception ex){
            ex.printStackTrace();

        }
        return null;
//        526
//        down vote
//        accepted
//        is your friend. Call getInstance("MD5")
//        macMD5 := md5.New()
//        macMD5.Write([]byte(strings.TrimSpace(string(macAddrOut))))
//        macStr := hex.EncodeToString(macMD5.Sum(nil))
//        return fmt.Sprintf("%v\\%v", buildVer, macStr)
    }

    public static void main(String[] args)throws Exception{
        MAC mh=new MAC();
        System.out.println(mh.getHashed());
    }
}
