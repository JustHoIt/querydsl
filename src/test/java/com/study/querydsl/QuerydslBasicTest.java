package com.study.querydsl;


import com.querydsl.jpa.impl.JPAQueryFactory;
import com.study.querydsl.entity.Member;
import com.study.querydsl.entity.QMember;
import com.study.querydsl.entity.Team;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import static com.study.querydsl.entity.QMember.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory qFactory;

    @BeforeEach
    public void before() {
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        //JPQL member1 찾기
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        //Querydsl member1 찾기
        // 1. JPAQueryFactory 에 EntityManager(em)을 넘겨줘야한다.
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        QMember qMember = new QMember("m");
        // 1. Q클래스를 static import 하면  코드를 깔끔하게 작성할 수 있다.
        // * 컴파일 시점에서 오류를 발견해준다.
        Member findMember = queryFactory
                .select(qMember)
                .from(qMember)
                .where(qMember.username.eq("member1"))
                .fetchOne();
        // JPQL에서는 파라미터를 바인딩 해줘야하지만 Querydsl에서는 자동으로 처리한다.
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl2() {
        //Querydsl2 member1 찾기 (코드 간결하게 만들기)
        // 2. JPAQueryFactory는 필드에서 선언해줘도 된다.
        qFactory = new JPAQueryFactory(em);


        // 1. Q클래스를 static import 하면  코드를 깔끔하게 작성할 수 있다.
        // * 컴파일 시점에서 오류를 발견해준다.
        Member findMember = qFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        // JPQL에서는 파라미터를 바인딩 해줘야하지만 Querydsl에서는 자동으로 처리한다.
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        qFactory = new JPAQueryFactory(em);
        Member findMember = qFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();
        /*
        * eq() // username = 'member1'
        * ne() // username != 'member1'
        * eq().not() // username != 'member1'
        * isNotNull() // username is not null
        * in(10,20) // age in(10,20)
        * notIn(10,20) // age not in(10, 20)
        * between(10,30) // between 10 ~ 30
        * goe(30) // >= 30
        * gt(30) // > 30
        * loe(30) // <= 30
        * lt(30) // < 30
        * like("string%") // like
        * contains("string") // like '%string%' 검색
        * startsWith("string") // like 'string%' 검색
        * */

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam(){
        qFactory = new JPAQueryFactory(em);
        Member findMember = qFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

}
