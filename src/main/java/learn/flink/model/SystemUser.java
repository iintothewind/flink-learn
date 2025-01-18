package learn.flink.model;

import lombok.*;

import java.io.Serializable;

@Builder
@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class SystemUser implements Serializable {
   private Long id;
   private String username;
   private String password;
   private String email;
}
