package com.easou;

import com.easou.interfaces.BIModel;

/**
 * ClassName App
 * 功能: 程序主入口
 * 运行方式与参数: TODO
 * Author yangweifeng
 * Date 2018/11/26 14:08
 * Version 1.0
 **/
public class App {
    public static void main(String[] args) throws Exception{
        long start_time = System.currentTimeMillis();
        if (args.length == 0){
            System.out.println("至少指定一个参数: 运行主类");
            return;
        }
        BIModel mainClass = (BIModel) Class.forName(args[0]).newInstance();
        String[] codeArgs = new String[args.length - 1];
        for (int i = 0; i < codeArgs.length; i++) {
            codeArgs[i] = args[i + 1];
        }
        mainClass.execute(codeArgs);
        long end_time = System.currentTimeMillis();
        System.out.println("job running cost time is " + (end_time - start_time) / 1000 + " s!");
    }
}
