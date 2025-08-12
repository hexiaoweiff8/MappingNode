package org.mappingnode.idmapping.db;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class DataSourceAop {
	@Pointcut("!@annotation(org.mappingnode.idmapping.db.Master) " +
            "&& (execution(* org.mappingnode.*.service..*.select*(..)) " +
            "|| execution(* org.mappingnode.*.service..*.get*(..)))")
    public void readPointcut() {

    }

    @Pointcut("@annotation(org.mappingnode.idmapping.db.Master) " +
            "|| execution(* org.mappingnode.*.service..*.insert*(..)) " +
            "|| execution(* org.mappingnode.*.service..*.add*(..)) " +
            "|| execution(* org.mappingnode.*.service..*.update*(..)) " +
            "|| execution(* org.mappingnode.*.service..*.edit*(..)) " +
            "|| execution(* org.mappingnode.*.service..*.delete*(..)) " +
            "|| execution(* org.mappingnode.*.service..*.remove*(..))")
    public void writePointcut() {

    }

    @Before("readPointcut()")
    public void read() {
    	DataSourceSwitcher.setSlave();
    }

    @Before("writePointcut()")
    public void write() {
    	DataSourceSwitcher.setMaster();
    }
}
