All code MUST ALWAYS follow the `naming-things` guidelines!

Use the following command to access the guidelines:

```console
curl -sSL https://raw.githubusercontent.com/codingjoe/naming-things/refs/heads/main/README.md | cat
```

The MOST IMPORTANT review considerations are:

- Consistency – We never lose data, even if someone unplugs the power or network.
- Durability – We recover from any failures, even poorly written tasks.
- Overhead – We focus resources on running tasks, not on managing the scheduler.
- Utilization – We keep the CPU saturated with tasks, not with idle time or waiting for locks.
