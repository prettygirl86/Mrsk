package com.mdo.kafka.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdo.kafka.config.RedisConfig;
import com.mdo.kafka.domain.Tasks;

@Service
public class StreamService {

	@Autowired
	protected RedisConfig redisConfig;

	/*public String insertTask(String msg) {
		String key = "100";
		String value = msg;
		RedisTemplate<Object, Object> redisTemplate = redisConfig
				.redisTemplate();
		redisTemplate.opsForValue().set(key, value);
		redisTemplate.expire( key, 60, TimeUnit.SECONDS );
		Object redisValue = redisTemplate.opsForValue().get(key);
		
		System.out.println("Redis retrieved value is=======" + redisValue);
		System.out.println("Redis retrieved string value is======="
				+ redisValue.toString());
		return "Success";
	}*/
	
	
	public String insertTask(String msg) {
		ObjectMapper mapper = new ObjectMapper();
		//JSON from String to Object
		Tasks tasks = new Tasks();
		try {
			tasks = mapper.readValue(msg, Tasks.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String key = Integer.toString(tasks.getTaskId());
		String value = msg;
		RedisTemplate<Object, Object> redisTemplate = redisConfig
				.redisTemplate();
		redisTemplate.opsForValue().set(key, value);
		//redisTemplate.expire( key, 600, TimeUnit.SECONDS );
		redisTemplate.expire( key, 60, TimeUnit.SECONDS );
		Object redisValue = redisTemplate.opsForValue().get(key);
		//Object redisValue = getValue(key);
		System.out.println("Redis retrieved value is=======" + redisValue);
		System.out.println("Redis retrieved string value is======="
				+ redisValue.toString());
		return "Success";
	}
	
	public String updateTaskToRedis(String msg) {
		ObjectMapper mapper = new ObjectMapper();
		//JSON from String to Object
		Tasks tasks = new Tasks();
		try {
			tasks = mapper.readValue(msg, Tasks.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String key = Integer.toString(tasks.getTaskId());
		String value = msg;
		RedisTemplate<Object, Object> redisTemplate = redisConfig
				.redisTemplate();
		redisTemplate.opsForValue().set(key, value);
		redisTemplate.expire( key, 60, TimeUnit.SECONDS );
		Object redisValue = redisTemplate.opsForValue().get(key);
		//Object redisValue = getValue(key);
		System.out.println("Redis retrieved value is=======" + redisValue);
		System.out.println("Redis retrieved string value is======="
				+ redisValue.toString());
		return "Success";
	}
	
	public String insertTaskToRedis(String msg) throws JsonParseException, JsonMappingException, IOException {
		
		ObjectMapper mapper = new ObjectMapper();
		//JSON from String to Object
		Tasks obj = mapper.readValue(msg, Tasks.class);
		setTask(obj);
		
		return "Success";
	}

	public Object getValue(String key) {
		RedisTemplate<Object, Object> redisTemplate = redisConfig
				.redisTemplate();
		Object object = redisTemplate.opsForValue().get(key);
		

		return object;
	}
	
	public void setTask( final Tasks tasks ) {
		
		
		RedisTemplate<Object, Object> redisTemplate = redisConfig
				.redisTemplate();
		 final String key = String.format( "tasks:%s", Integer.toString(tasks.getTaskId() ) );
		// final String key = Integer.toString(tasks.getTaskId());
		 final Map< String, Object > properties = new HashMap< String, Object >();
		 
		 properties.put( "taskId", tasks.getTaskId());
		 properties.put( "documentId", tasks.getDocumentId());
		 properties.put( "type", tasks.getType() );
		 properties.put( "status", tasks.getStatus());
		 properties.put( "taskItem", tasks.getTaskItem());
		 properties.put( "taskItemStatus", tasks.getTaskItemStatus());
		 properties.put( "taskItemUser", tasks.getTaskItemUser());
		 properties.put( "taskItemTimeStamp", tasks.getTaskItemTimeStamp());
		 properties.put( "urgent", tasks.isUrgent());
		 
		
		 redisTemplate.opsForHash().putAll( key, properties);
		 redisTemplate.expire( key, 60, TimeUnit.SECONDS );
		 
		// final String key = String.format( "user:%s", id );
		 final String taskkey = String.format( "tasks:%s", Integer.toString(tasks.getTaskId() ));

		 final int documentId = ( Integer )redisTemplate.opsForHash().get(taskkey, "documentId");
		 
		 System.out.println("redis retrieved documentId value is=======" + documentId);
				 
		 
		//Object redisValue = redisTemplate.opsForValue().get(key);
		//System.out.println("redis retrieved value is=======" + redisValue);
			//System.out.println("Redis retrieved string value is======="
				//	+ redisValue.toString());
		}

}
