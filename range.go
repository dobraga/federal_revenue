package main

func Range(ini, end, step int) [][2]int {
	r := [][2]int{}
	i := 0

	for i < end {
		end_step := i + step - 1
		if end_step > end {
			end_step = end
		}

		r = append(r, [2]int{i, end_step})
		i += step
	}

	return r
}
