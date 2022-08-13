package com.gao.es;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ：gaozhiqi
 * @date ：2022/8/13 15:29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Article {
    /**
     * 主键id
     */
    private Long id;
    /**
     * 标题
     */
    private String title;
    /**
     * 内容
     */
    private String content;

}
