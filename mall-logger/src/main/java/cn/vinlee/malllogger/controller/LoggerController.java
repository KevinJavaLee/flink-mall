package cn.vinlee.malllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Vinlee Xiao
 * @className LoggerController
 * @description 日志控制器
 * @date 2022/7/6
 **/
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test1() {
        return "success";
    }

    @RequestMapping("user")
    public String getUser(@RequestParam("name") String name, @RequestParam("sex") String sex) {
        System.out.println(name);
        System.out.println(sex);
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {

        //落盘
        log.info(jsonStr);
        //写入kafka中
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }
}
