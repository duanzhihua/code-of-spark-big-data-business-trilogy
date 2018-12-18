package com.telecom.ronghezhifu.cores

/**
  * Spark商业案例书稿第六章：6.1.5.2  Spark在通信运营商生产环境中的应用案例代码
  * 版权：DT大数据梦工厂所有
  * 时间：2017年4月19日；
  *
  ***/

/**
  * A collection of regexes for Ronghezhifu LOG extracting information from the rhzfline string.
  */

object rhzfLogRegex {
  /**
    * Regular expression used for Scene :
    * Log analysis:extraction the total time of the RHZF system to UAM
    *2017-01-15 17:47:56,242 [ERROR] [MQAuthOperateBusiness.java] : 145 -- 去UAM认证的业务的时间: >>>> Auth >>>> [31]毫秒
    */
  val RHZF_UAM_TIME_REGEX =
    """^(\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+) >>>> (\S+) >>>> (\S+)](\S+)$""".r
  /**
    * Regular expression used for Scene :
    * Log analysis:extraction the error code and the error description of the RHZF system  received from  IT's charging system
    * 2017-01-15 17:47:58,430 [DEBUG] [MQQueryInvoiceBusiness.java] : 498 -- 流水号:372@@8698,queryInvoice response:<soap:Envelope xmlns:soap="http://@@/soap/envelope/"><soap:Body><ns2:queryInvoiceResponse xmlns:ns2="http://@@.sh.cn/"><response><head><errCode>00000001</errCode><errDesc>com.@@.exception.ApplicationException: 账户不存在或当前不是有效状态
    */
  val RHZF_MQ_QueryInvoice_ErrCode_REGEX =
    """^(\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+):(\S+),(\S+) (\S+) (\S+) (.*)<response><head><errCode>([0-9]*)</errCode><errDesc>(.*): (\S+)$""".r

  /**
    * Regular expression used for Scene :
    * 1: Log analysis:extraction the MQ check order time of the RHZF system to IT's MQ system
    *   2017-01-15 17:41:21,654 [ERROR] [MQBillCheckBusiness.java] : 233 -- 流水号:20170@@636 MQ校验订单业务的时间: >>>> CheckBill >>>> [554]毫秒
    * 2:Log analysis:extraction the MQ billing payment time of the RHZF system to IT's MQ system
    * 2016-12-15 15:14:30,263 [ERROR] [MQBillPaymentBusiness.java] : 182 -- 流水号:2016@@41202 MQ账单支付业务的时间: >>>> Payment >>>> [423]毫秒
    *
    */
  val RHZF_MQ_CHECK_ORDER_TIME_REGEX=
    """^(\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+):(\S+) (\S+) >>>> (\S+) >>>> (\S+)](\S+)$""".r

  /**
    * Regular expression used for Scene 4:
    *Log analysis:extraction the error code and the error description of the RHZF system  received from  IT's charging system
    * 2016-12-15 21:31:52,015 [WARN ] [MQQueryAccountNoBusiness.java] : 278 -- 流水号:1008@@27,queryAccountNo response:<soap:Envelope xmlns:soap="http://@@/soap/envelope/"><soap:Body><ns2:queryAccountNoResponse xmlns:ns2="http://@@.sh.cn/"><response><head><errCode>00000001</errCode><errDesc>java.lang.Exception: 设备号对应帐号为空
    */
  val RHZF_MQ_queryAccountNo_ErrCode_REGEX =
    """^(\S+) (\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+):(\S+),(\S+) (\S+) (\S+) (.*)<response><head><errCode>([0-9]*)</errCode><errDesc>(.*): (\S+)$""".r

}
