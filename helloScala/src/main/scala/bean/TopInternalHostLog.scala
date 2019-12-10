package bean

case class TopInternalHostLog(source: String,
                              session_num:Int,
                              order_by:String,
                              c2s_pkt_num:Int,
                              S2c_pkt_num:Int,
                              c2s_byte_num:Int,
                              s2c_byte_num:Int,
                              __time:Long)
