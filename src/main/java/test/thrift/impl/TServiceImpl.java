package test.thrift.impl;

import org.apache.thrift.TException;
import test.thrift.api.TService;

/**
 * Created by nihao on 16/11/27.
 */
public class TServiceImpl implements TService.Iface {
    @Override
    public String query(String name) throws TException {
        System.out.println(name+" coming......");
        return "hello,"+name;
    }
}
