import subprocess

# stack_ref = pulumi.StackReference("bernardo-costa-loft/desafio2-infra/dev")
# raw_bucket_name = _sync_await(stack_ref.get_output_details("raw_bucket_name")).value
# print(raw_bucket_name)
print(subprocess
      .run(
          ["poetry","run","pulumi","stack","output","raw_bucket_name"], 
          capture_output=True
        ).stdout.decode(encoding="utf-8")
)