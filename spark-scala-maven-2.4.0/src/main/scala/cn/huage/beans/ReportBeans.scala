package cn.huage.beans

/**
  * @author zhangjin
  * @create 2018-12-02 10:46
  */
case class RptArea(
                    pName: String,
                    cName: String,
                    rawReq: Int,
                    effReq: Int,
                    adReq: Int,
                    rtbReq: Int,
                    rbtSuccReq: Int,
                    adShow: Int,
                    adClick: Int,
                    adCost: Double,
                    adExpense: Double
                  )


case class RptDeviceIspName(
                             ispName: String,
                             rawReq: Int,
                             effReq: Int,
                             adReq: Int,
                             rtbReq: Int,
                             rbtSuccReq: Int,
                             adShow: Int,
                             adClick: Int,
                             adCost: Double,
                             adExpense: Double
                           )


case class RptAppIspName(
                          appName: String,
                          rawReq: Int,
                          effReq: Int,
                          adReq: Int,
                          rtbReq: Int,
                          rbtSuccReq: Int,
                          adShow: Int,
                          adClick: Int,
                          adCost: Double,
                          adExpense: Double
                        )
