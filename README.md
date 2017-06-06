# DynamicDataSource
基于spring 事务的读写分离数据源，并包含从库可用性管理

## 简介
* 数据源可以配置一个主库和多个从库，从库使用RR策略负载
* 管理从库可用性，自动下线无法访问、同步状态不正确的从库，并在它们可以正常提供正常服务的时候上线
* 读写分离基于事务，sql在主库还是从库执行取决于事务开启时的readonly设置
* 用户可自定义的消息通知接口

## 使用
#### 1. 引用
#### 2. 增加dataSource配置
```
 <bean id="dataSource_writeable" class="xxxxxxxxxxxxxxxxxxxx">
 ***
 </bean>
 <bean id="dataSource_readonly1" class="xxxxxxxxxxxxxxxxxxxx">
 ***
 </bean>
 <bean id="dataSource_readonly2" class="xxxxxxxxxxxxxxxxxxxx">
 ***
 </bean>
 <bean id="dataSource" class="com.sven.dynamicdatasource.DynamicMysqlDataSource" destroy-method="onDetory">
		<property name="master" ref="dataSource_writeable" />		
		<property name="allSlaveMap">
		 	<map>
		 		<entry key="192.168.1.21:3306" value-ref="dataSource_readonly1"></entry>
		 		<entry key="192.168.1.22:3306" value-ref="dataSource_readonly2"></entry>
		 	</map>
		</property>
		<property name="validationQueryTimeout" value="5"></property>
 </bean>
```
#### 3. 更改transactionManager配置
```
<bean id="transactionManager" class="com.sven.dynamicdatasource.DynamicDataSourceTransactionManager">
		<property name="dataSource" ref="dataSource" />
		<property name="beginTransactionTimeOut" value="10"></property>
</bean>
```
#### 4.3、	把需要读写分离的service类的事务属性设置成readOnly=true
基于注解的例子
```
@Transactional(readOnly = true)
public TestObject getById(String id){*****}
```
*使用主库还是从库完全取决于事务开启时设置的readonly ，如果是PROPAGATION_REQUIRED加入调用栈中的另一个事务此方法配置的readonly是无效的。所以如果有写入需要，则最事务最开始的时候就应该标记成readonly=false ，否则会报从库不能写入的错误*
