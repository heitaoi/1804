package com.run.common.factory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.io.Resource;


import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class JedisClusterFactory implements FactoryBean<JedisCluster>{
	private JedisPoolConfig poolConfig;
	private Resource addressConfig;
	private String addressKeyPrefix;

	@Override
	public JedisCluster getObject() throws Exception {
		Set<HostAndPort> nodes = getNodes();
		JedisCluster jedis = new JedisCluster(nodes, poolConfig);
		return jedis;
	}

	private Set<HostAndPort> getNodes() {
		Set<HostAndPort> nodes = new HashSet<>();
		Properties properties = new Properties();
		try {
			properties.load(addressConfig.getInputStream());
			for (Object node : properties.keySet()) {
				String strNode = (String)node;
				if(strNode.startsWith(addressKeyPrefix)){
					String value = properties.getProperty(strNode);
					String[] args = value.split(":");
					nodes.add(new HostAndPort(args[0], Integer.parseInt(args[1])));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return nodes;
	}

	@Override
	public Class<?> getObjectType() {
		return JedisCluster.class;
	}

	@Override
	public boolean isSingleton() {
		return false;
	}

	public JedisPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(JedisPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	public Resource getAddressConfig() {
		return addressConfig;
	}

	public void setAddressConfig(Resource addressConfig) {
		this.addressConfig = addressConfig;
	}

	public String getAddressKeyPrefix() {
		return addressKeyPrefix;
	}

	public void setAddressKeyPrefix(String addressKeyPrefix) {
		this.addressKeyPrefix = addressKeyPrefix;
	}
	
	
	

}
