/**
 * 
 */
package za.co.sindi.data.microstream.repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

import one.microstream.persistence.types.Persister;

/**
 * @author Buhake Sindi
 * @since 19 November 2021
 */
public abstract class AbstractMicrostreamRepository<E, ID> implements MicrostreamRepository<E, ID> {

	protected final Map<ID, E> entityMap = new ConcurrentHashMap<>();
	
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
//	private final Persister persister;
	
//	private final String location;

//	/**
//	 * @param location
//	 */
//	protected AbstractMicrostreamRepository(String location) {
//		super();
//		Objects.requireNonNull(location, "No location was specified.");
////		this.location = location;
//		persister = EmbeddedStorage.start(entityMap, Paths.get(location));
//	}
//
//	/**
//	 * @param persister
//	 */
//	protected AbstractMicrostreamRepository(Persister persister) {
//		super();
//		this.persister = persister;
//	}

	@Override
	public Optional<E> findById(ID id) {
		// TODO Auto-generated method stub
		return readWithLock(() -> Optional.ofNullable(entityMap.get(id)));
	}

	@Override
	public boolean existsById(ID id) {
		// TODO Auto-generated method stub
		return readWithLock(() -> entityMap.containsKey(id));
	}

	@Override
	public Stream<E> findAllById(Iterable<ID> ids) {
		// TODO Auto-generated method stub
		return readWithLock(() -> {
			Objects.requireNonNull(ids);
			List<E> results = new ArrayList<>();
			ids.forEach(id -> {
				if (entityMap.containsKey(id)) 
					results.add(entityMap.get(id));
			});
			
			return Collections.unmodifiableList(results).stream();
		});
	}

	@Override
	public void deleteById(ID id) {
		// TODO Auto-generated method stub
		writeWithLock(() -> {
			entityMap.remove(id);
			store();
			return (Void)null;
		});
	}

	@Override
	public void deleteAllById(Iterable<ID> ids) {
		// TODO Auto-generated method stub
		writeWithLock(() -> {
			Objects.requireNonNull(ids);
			ids.forEach(id -> {
				if (entityMap.containsKey(id)) 
					entityMap.remove(id);
			});
			
			store();
			return (Void)null;
		});
	}

	@Override
	public long count() {
		// TODO Auto-generated method stub
		return readWithLock(() -> entityMap.size());
	}

	@Override
	public void deleteAll() {
		// TODO Auto-generated method stub
		writeWithLock(() -> {
			entityMap.clear();
			store();
			return (Void)null;
		});
	}

	@Override
	public Stream<E> findAll() {
		// TODO Auto-generated method stub
		return readWithLock(() -> entityMap.values().stream());
	}

	
	protected final <T> T writeWithLock(Supplier<T> supplier) {
		final WriteLock writeLock = lock.writeLock();
		writeLock.lock();
		try {
			return supplier.get();
		} finally {
			writeLock.unlock();
		}
	}
	
	protected final <T> T readWithLock(Supplier<T> supplier) {
		final ReadLock readLock = lock.readLock();
		readLock.lock();
		try {
			return supplier.get();
		} finally {
			readLock.unlock();
		}
	}
	
	protected void store() {
		final Persister persister = Objects.requireNonNull(getPersister(), "No MicroStream Persister (or StorageManager) was provided.");
		persister.store(entityMap);
	}
	
	protected abstract Persister getPersister();
}
