/**
 * 
 */
package za.co.sindi.data.microstream.repository;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import za.co.sindi.data.entity.IdentifiableEntity;

/**
 * @author Buhake Sindi
 * @since 19 November 2023
 */
public abstract class IdentifiableMicrostreamRepository<E extends IdentifiableEntity<ID>, ID extends Comparable<ID> & Serializable> extends AbstractMicrostreamRepository<E, ID> {

//	/**
//	 * @param persister
//	 */
//	protected IdentifiableMicrostreamRepository(Persister persister) {
//		super(persister);
//		// TODO Auto-generated constructor stub
//	}
//
//	/**
//	 * @param location
//	 */
//	protected IdentifiableMicrostreamRepository(String location) {
//		super(location);
//		// TODO Auto-generated constructor stub
//	}

	@Override
	public void delete(E entity) {
		// TODO Auto-generated method stub
		writeWithLock(() -> {
			if (entity != null && entity.getId() != null) {
				if (entityMap.remove(entity.getId()) != null) {
					store();
				}
			}
			return (Void)null;
		});
	}

	@Override
	public void deleteAll(Iterable<? extends E> entities) {
		// TODO Auto-generated method stub
		writeWithLock(() -> {
			if (entities != null) {
				int previousTotal = entityMap.size();
				entities.forEach(entity -> {
					if (entity != null && entity.getId() != null)
						entityMap.remove(entity.getId());
				});
				if (entityMap.size() != previousTotal) {
					store();
				}
			}
			return (Void)null;
		});
	}

	@Override
	public <S extends E> S save(S entity) {
		// TODO Auto-generated method stub
		return writeWithLock(() -> {
			if (entity != null && entity.getId() != null) {
				if (!entityMap.containsKey(entity.getId())) {
					entityMap.put(entity.getId(), entity);
					store();
				}
			}
			return entity;
		});
	}

	@Override
	public <S extends E> Iterable<S> saveAll(Iterable<S> entities) {
		// TODO Auto-generated method stub
		return writeWithLock(() -> {
			final AtomicInteger total = new AtomicInteger();
			if (entities != null) {
				entities.forEach(entity -> {
					if (entity != null && entity.getId() != null && !entityMap.containsKey(entity.getId())) {
						entityMap.put(entity.getId(), entity);
						total.incrementAndGet();
					}
				});
				if (total.get() > 0) store();
			}
			return entities;
		});
	}

	@Override
	public boolean update(E entity) {
		// TODO Auto-generated method stub
		return writeWithLock(() -> {
			if (entity != null && entity.getId() != null && entityMap.containsKey(entity.getId())) {
				entityMap.replace(entity.getId(), entity);
				store();
				return true;
			}
			return false;
		});
	}

	@Override
	public int updateAll(Iterable<E> entities) {
		// TODO Auto-generated method stub
		return writeWithLock(() -> {
			final AtomicInteger total = new AtomicInteger();
			if (entities != null) {
				entities.forEach(entity -> {
					if (entity != null && entity.getId() != null && entityMap.containsKey(entity.getId())) {
						entityMap.replace(entity.getId(), entity);
						total.incrementAndGet();
					}
				});
				store();
			}
			return total.get();
		});
	}
}
