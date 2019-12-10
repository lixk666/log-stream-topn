package bean;

import com.alibaba.fastjson.annotation.JSONField;

public class InternalLog {

    private String source;
    private int session_num;
    private String order_by;
    private int c2s_pkt_num;
    private int S2c_pkt_num;
    private int c2s_byte_num;
    private int s2c_byte_num;
    private long __time;

    @JSONField(name="client_ip")
    public String getSource() {
        return source;
    }
    @JSONField(name="client_ip")
    public void setSource(String source) {
        this.source = source;
    }

    public int getSession_num() {
        return session_num;
    }

    public void setSession_num(int session_num) {
        this.session_num = session_num;
    }

    public String getOrder_by() {
        return order_by;
    }

    public void setOrder_by(String order_by) {
        this.order_by = order_by;
    }

    public int getC2s_pkt_num() {
        return c2s_pkt_num;
    }

    public void setC2s_pkt_num(int c2s_pkt_num) {
        this.c2s_pkt_num = c2s_pkt_num;
    }

    public int getS2c_pkt_num() {
        return S2c_pkt_num;
    }

    public void setS2c_pkt_num(int s2c_pkt_num) {
        S2c_pkt_num = s2c_pkt_num;
    }

    public int getC2s_byte_num() {
        return c2s_byte_num;
    }

    public void setC2s_byte_num(int c2s_byte_num) {
        this.c2s_byte_num = c2s_byte_num;
    }

    public int getS2c_byte_num() {
        return s2c_byte_num;
    }

    public void setS2c_byte_num(int s2c_byte_num) {
        this.s2c_byte_num = s2c_byte_num;
    }
    @JSONField(name="recv_time")
    public long get__time() {
        return __time;
    }
    @JSONField(name="recv_time")
    public void set__time(long __time) {
        this.__time = __time;
    }


    @Override
    public String toString() {
        return "InternalLog{" +
                "source='" + source + '\'' +
                ", session_num=" + session_num +
                ", order_by='" + order_by + '\'' +
                ", c2s_pkt_num=" + c2s_pkt_num +
                ", S2c_pkt_num=" + S2c_pkt_num +
                ", c2s_byte_num=" + c2s_byte_num +
                ", s2c_byte_num=" + s2c_byte_num +
                ", __time=" + __time +
                '}';
    }
}
